import streamlit as st
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
import io
import csv

KISHOU_XML_PAGE_URL = "https://www.data.jma.go.jp/developer/xml/feed/extra_l.xml"

st.set_page_config(page_title="気象庁 防災情報 (XML) ビューア", layout="wide")

@st.cache_data(ttl=600)
def fetch_feed(url: str, hours_threshold: int = 48):
    fetched = {"main_feed_xml": None, "linked_entries_xml": []}
    time_threshold = datetime.now(timezone.utc) - timedelta(hours=hours_threshold)

    try:
        resp = requests.get(url, timeout=15)
        resp.raise_for_status()
        fetched["main_feed_xml"] = resp.content

        root = ET.fromstring(fetched["main_feed_xml"].decode("utf-8"))
        atom_ns = "{http://www.w3.org/2005/Atom}"

        for entry in root.findall(f"{atom_ns}entry"):
            entry_info = {}
            entry_info["EntryID"] = entry.find(f"{atom_ns}id").text if entry.find(f"{atom_ns}id") is not None else "N/A"
            entry_info["FeedReportDateTime"] = entry.find(f"{atom_ns}updated").text if entry.find(f"{atom_ns}updated") is not None else "N/A"
            entry_info["FeedTitle"] = entry.find(f"{atom_ns}title").text if entry.find(f"{atom_ns}title") is not None else "N/A"
            entry_info["Author"] = entry.find(f"{atom_ns}author/{atom_ns}name").text if entry.find(f"{atom_ns}author/{atom_ns}name") is not None else "N/A"

            # 時刻フィルタ
            feed_report_time_str = entry_info.get("FeedReportDateTime")
            skip_by_time = False
            if feed_report_time_str and feed_report_time_str != "N/A":
                try:
                    if feed_report_time_str.endswith("Z"):
                        feed_report_time = datetime.fromisoformat(feed_report_time_str[:-1]).replace(tzinfo=timezone.utc)
                    else:
                        feed_report_time = datetime.fromisoformat(feed_report_time_str)
                    if feed_report_time < time_threshold:
                        skip_by_time = True
                except Exception:
                    pass

            linked_xml_link_element = entry.find(f'{atom_ns}link[@type="application/xml"]')
            if linked_xml_link_element is not None and not skip_by_time:
                linked_xml_url = linked_xml_link_element.get("href")
                if linked_xml_url:
                    try:
                        lx_resp = requests.get(linked_xml_url, timeout=15)
                        lx_resp.raise_for_status()
                        entry_info["LinkedXMLData"] = lx_resp.content
                        entry_info["LinkedXMLUrl"] = linked_xml_url
                    except Exception as e:
                        entry_info["LinkedXMLData"] = None
                        entry_info["LinkedXMLError"] = str(e)
                else:
                    entry_info["LinkedXMLData"] = None
            else:
                entry_info["LinkedXMLData"] = None
            fetched["linked_entries_xml"].append(entry_info)

    except Exception as e:
        fetched["error"] = str(e)

    return fetched

def parse_warnings_advisories(fetched_data, hours_threshold: int = 48):
    parsed = []
    if not fetched_data or not fetched_data.get("linked_entries_xml"):
        return parsed

    time_threshold = datetime.now(timezone.utc) - timedelta(hours=hours_threshold)

    for entry in fetched_data["linked_entries_xml"]:
        feed_title = entry.get("FeedTitle", "N/A")
        if feed_title != "気象特別警報・警報・注意報":
            continue

        feed_time_str = entry.get("FeedReportDateTime")
        try:
            if feed_time_str and feed_time_str.endswith("Z"):
                feed_time = datetime.fromisoformat(feed_time_str[:-1]).replace(tzinfo=timezone.utc)
            elif feed_time_str:
                feed_time = datetime.fromisoformat(feed_time_str)
            else:
                feed_time = None
        except Exception:
            feed_time = None

        if feed_time and feed_time < time_threshold:
            continue

        extracted = {
            "EntryID": entry.get("EntryID", "N/A"),
            "FeedReportDateTime": entry.get("FeedReportDateTime", "N/A"),
            "FeedTitle": feed_title,
            "Author": entry.get("Author", "N/A"),
            "LinkedXMLDataPresent": bool(entry.get("LinkedXMLData")),
            "LinkedXMLUrl": entry.get("LinkedXMLUrl", "")
        }

        linked_bytes = entry.get("LinkedXMLData")
        warnings = []
        report_dt = extracted["FeedReportDateTime"]

        if linked_bytes:
            try:
                xml_text = linked_bytes.decode("utf-8")
            except Exception:
                xml_text = linked_bytes.decode("utf-8", errors="replace")
            try:
                root = ET.fromstring(xml_text)
                rt = root.find('.//{*}ReportDateTime')
                if rt is not None and rt.text:
                    report_dt = rt.text

                headline = root.find('.//{*}Headline/{*}Text')
                overall_detail = headline.text if headline is not None and headline.text else "N/A"

                items = root.findall('.//{*}Item')
                for item in items:
                    kind_el = item.find('.//{*}Kind/{*}Name')
                    area_el = item.find('.//{*}Areas/{*}Area/{*}Name')
                    if area_el is None:
                        area_el = item.find('.//{*}Areas/{*}Area/{*}Prefecture/{*}Name')

                    kind = kind_el.text if kind_el is not None and kind_el.text else "N/A"
                    area = area_el.text if area_el is not None and area_el.text else "N/A"

                    if kind != "N/A" or area != "N/A":
                        warnings.append({"Kind": kind, "Area": area, "Detail": overall_detail})
            except ET.ParseError:
                warnings.append({"Kind": "解析エラー", "Area": "解析エラー", "Detail": "XML解析エラー"})
            except Exception:
                warnings.append({"Kind": "エラー", "Area": "エラー", "Detail": "不明なエラー"})
        else:
            warnings.append({"Kind": "取得失敗", "Area": "取得失敗", "Detail": "リンクXMLがありません"})

        if warnings and extracted["LinkedXMLDataPresent"]:
            extracted["ReportDateTime"] = report_dt
            extracted["WarningsAdvisories"] = warnings
            parsed.append(extracted)

    return parsed

st.title("気象庁 防災情報 (XML) ビューア")

col1, col2 = st.columns([1, 2])
with col1:
    st.markdown("### 設定")
    hours = st.number_input("何時間以内のフィードを取得しますか？", min_value=1, max_value=168, value=48, step=1)
    if st.button("フィード取得 / 更新"):
        st.experimental_rerun()

with col2:
    st.markdown("### フィード取得状況")
    with st.spinner("フィードを取得しています..."):
        data = fetch_feed(KISHOU_XML_PAGE_URL, hours_threshold=hours)

if data.get("error"):
    st.error(f"取得中にエラーが発生しました: {data['error']}")

entries = data.get("linked_entries_xml", [])
st.markdown(f"**取得エントリー数**: {len(entries)}")

if entries:
    titles = [f"{i+1}: {e.get('FeedTitle','N/A')} ({e.get('FeedReportDateTime','')})" for i, e in enumerate(entries)]
    idx = st.selectbox("エントリーを選択", options=list(range(len(entries))), format_func=lambda i: titles[i])
    sel = entries[idx]

    st.subheader("メタ情報")
    st.write({
        "EntryID": sel.get("EntryID"),
        "FeedTitle": sel.get("FeedTitle"),
        "FeedReportDateTime": sel.get("FeedReportDateTime"),
        "Author": sel.get("Author"),
        "LinkedXMLUrl": sel.get("LinkedXMLUrl", "N/A"),
    })

st.markdown("---")
st.header("気象特別警報・警報・注意報 の抽出結果")
parsed = parse_warnings_advisories(data, hours_threshold=hours)
st.markdown(f"**抽出件数**: {len(parsed)}")

if parsed:
    rows = []
    for p in parsed:
        for wa in p.get("WarningsAdvisories", []):
            rows.append({
                "EntryID": p.get("EntryID"),
                "FeedReportDateTime": p.get("FeedReportDateTime"),
                "ReportDateTime": p.get("ReportDateTime", ""),
                "FeedTitle": p.get("FeedTitle", ""),
                "Author": p.get("Author", ""),
                "LinkedXMLUrl": p.get("LinkedXMLUrl", ""),
                "Kind": wa.get("Kind"),
                "Area": wa.get("Area"),
                "Detail": wa.get("Detail"),
            })
    st.table(rows)

    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer)
    header = ["EntryID", "FeedReportDateTime", "ReportDateTime", "FeedTitle", "Author", "LinkedXMLUrl", "Kind", "Area", "Detail"]
    writer.writerow(header)
    for r in rows:
        writer.writerow([r.get(h, "") for h in header])

    csv_bytes = csv_buffer.getvalue().encode("utf-8-sig")
    csv_buffer.close()

    file_name = f"warnings_{datetime.now().strftime('%Y%m%d%H%M%S')}.csv"
    st.download_button(
        label="CSV をダウンロード",
        data=csv_bytes,
        file_name=file_name,
        mime="text/csv"
    )

    sel_idx = st.selectbox("抽出エントリー（詳細）を選択", options=list(range(len(parsed))), format_func=lambda i: f"{i+1}: {parsed[i]['EntryID']} / {parsed[i]['FeedReportDateTime']}")
    sel_parsed = parsed[sel_idx]
    st.subheader("選択エントリーの詳細")
    st.json({
        "EntryID": sel_parsed.get("EntryID"),
        "FeedTitle": sel_parsed.get("FeedTitle"),
        "FeedReportDateTime": sel_parsed.get("FeedReportDateTime"),
        "ReportDateTime": sel_parsed.get("ReportDateTime"),
        "Author": sel_parsed.get("Author"),
        "LinkedXMLDataPresent": sel_parsed.get("LinkedXMLDataPresent"),
        "LinkedXMLUrl": sel_parsed.get("LinkedXMLUrl"),
        "WarningsAdvisories": sel_parsed.get("WarningsAdvisories"),
    })
else:
    st.info("抽出された '気象特別警報・警報・注意報' はありません。")
