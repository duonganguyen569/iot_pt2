"""Presentation-only dashboard; pipeline and processing rules are unchanged."""
import time
import pandas as pd
import streamlit as st
from iot import storage
from iot.config import SENSORS
from iot.metrics import summarize

st.set_page_config(page_title='IoT Observatory', page_icon='◉', layout='wide')
st.markdown('''<style>
.stApp {background:#f5f7fa;color:#182b42;}
.block-container {max-width:1440px;padding-top:2.2rem;padding-bottom:2rem;}
h1,h2,h3 {letter-spacing:-.025em;color:#182b42;}
h1 {font-size:2.5rem!important;} h3 {font-size:1.15rem!important;}
[data-testid="stMetric"] {background:white;border:1px solid #e2e8f0;border-radius:14px;padding:18px 20px;}
[data-testid="stMetricLabel"] {color:#53657b;}
[data-testid="stMetricValue"] {font-size:1.8rem;letter-spacing:-.035em;}
[data-testid="stVerticalBlockBorderWrapper"]>div {border-radius:14px;}
.stDownloadButton button {border-radius:9px;}
</style>''', unsafe_allow_html=True)
storage.init()
st.caption('IOT OBSERVATORY  /  PHÒNG THÍ NGHIỆM DỮ LIỆU')
st.title('Theo dõi cảm biến')
st.write('Từ tín hiệu gốc đến dữ liệu sạch — quan sát chất lượng, xu hướng và độ trễ trong cùng một nơi.')
LABELS = {'temperature': 'Nhiệt độ', 'humidity': 'Độ ẩm', 'light': 'Ánh sáng'}
UNITS = {'temperature': '°C', 'humidity': '%', 'light': 'đơn vị mô phỏng'}


def chart(frame, columns, names, colors, height=280):
    data = frame.set_index('time')[columns].apply(pd.to_numeric, errors='coerce')
    data.columns = names
    st.line_chart(data, color=colors, height=height, use_container_width=True)


@st.fragment(run_every='3s')
def dashboard():
    raw, processed = storage.raw_frame(), storage.processed_frame()
    mode = storage.get_state('storage_mode', 'Chưa xử lý dữ liệu')
    st.caption(f'{mode}  ·  Tự làm mới mỗi 3 giây  ·  Trục thời gian UTC')
    if raw.empty:
        st.info('Chưa có dữ liệu. Khởi động collector và simulator để bắt đầu quan sát.')
        return
    error = storage.get_state('worker_error')
    if error:
        st.error('Worker đang lỗi: ' + error)
    filters = st.columns([1, 2, 1])
    device = filters[0].selectbox('Thiết bị', sorted(raw.device_id.unique()))
    runs = raw.loc[raw.device_id == device, 'run_id'].drop_duplicates().tolist()
    run = filters[1].selectbox('Phiên chạy', runs, index=len(runs)-1)
    sensor = filters[2].selectbox('Cảm biến', SENSORS, format_func=lambda s: LABELS[s])
    r = raw[(raw.device_id == device) & (raw.run_id == run)].copy()
    p = processed.copy()
    if not p.empty:
        p = p[(p.device_id == device) & (p.run_id == run)].copy()
    metrics = summarize(r)
    r['time'] = pd.to_datetime(r.sent_at, unit='s', utc=True)
    if not p.empty:
        p['time'] = pd.to_datetime(p.timestamp, utc=True)
    age = max(0., time.time()-r.sent_at.max())
    last = float(storage.get_state('worker_success_at', '0'))
    st.caption(f"Phiên đã chọn: {r.time.min():%d/%m/%Y %H:%M:%S} – {r.time.max():%H:%M:%S} UTC  ·  Tuổi dữ liệu mới nhất: {age:,.0f} s")
    if last:
        st.caption(f'Worker xử lý thành công gần nhất: {max(0., time.time()-last):,.0f} giây trước. Tuổi dữ liệu không phải độ trễ hiển thị trình duyệt.')
    cards = st.columns(4)
    cards[0].metric('Bản tin duy nhất', f"{metrics['unique_messages']:,}")
    cards[1].metric('Bin sau xử lý', f'{len(p):,}')
    cards[2].metric('Trễ collector · trung bình', f"{metrics['latency_mean_ms']:.2f} ms")
    cards[3].metric('Trễ collector · P95', f"{metrics['latency_p95_ms']:.2f} ms")
    overview, quality, latency, records = st.tabs(['Tổng quan', 'Chất lượng & đặc trưng', 'Độ trễ & hệ thống', 'Dữ liệu & xuất CSV'])
    with overview:
        st.subheader(f'{LABELS[sensor]} · {UNITS[sensor]}')
        left, right = st.columns(2)
        with left, st.container(border=True):
            st.markdown('**Dữ liệu gốc**')
            st.caption('Giữ nguyên các giá trị bất thường để đối chiếu.')
            chart(r, [sensor], ['Dữ liệu gốc'], ['#4361c2'])
        with right, st.container(border=True):
            st.markdown('**Sau xử lý**')
            st.caption('Trung bình 10 giây và rolling mean 5 bin; trục Y tự điều chỉnh.')
            if p.empty:
                st.info('Đang chờ worker xử lý.')
            else:
                chart(p, [sensor, sensor+'_rolling_mean'], ['Sau xử lý', 'Rolling mean'], ['#0d887c', '#db9132'])
        if not p.empty:
            c = st.columns(3)
            c[0].metric('Giá trị thiếu gốc', int(p[sensor+'_missing'].sum()))
            c[1].metric('Ngoại lệ được phát hiện', int(p[sensor+'_outlier'].sum()))
            c[2].metric('Bin đã nội suy', int(p[sensor+'_imputed'].sum()))
        st.caption('Biểu đồ phục vụ quan sát; các bộ đếm không phải kiểm thử tự động tính đúng của thuật toán.')
    with quality:
        if p.empty:
            st.info('Đang chờ dữ liệu sau xử lý.')
        else:
            st.subheader('Chuẩn hóa theo miền cảm biến')
            st.caption('Nhiệt độ: −20–60 °C · Độ ẩm: 0–100% · Ánh sáng: 0–2000. Các miền là giả định của mô phỏng.')
            chart(p, [s+'_normalized' for s in SENSORS], [LABELS[s] for s in SENSORS], ['#4361c2', '#0d887c', '#db9132'])
            st.subheader(f'Biến thiên giữa hai bin · {LABELS[sensor]}')
            chart(p, [sensor+'_delta'], ['Delta'], ['#4361c2'], 220)
            st.caption('Nội suy chỉ áp dụng cho khoảng thiếu bên trong tối đa 3 bin. Khoảng dài và biên còn thiếu được giữ nguyên. IQR được tính lại trên phiên hiện có.')
    with latency:
        st.subheader('Từ publisher đến collector')
        chart(r, ['latency_ms'], ['Độ trễ (ms)'], ['#4361c2'])
        c = st.columns(3)
        c[0].metric('Trễ collector · lớn nhất', f"{metrics['latency_max_ms']:.2f} ms")
        c[1].metric('Khoảng trống sequence', metrics['sequence_gaps'])
        delay = metrics.get('to_processed_p95_ms')
        c[2].metric('Publisher → processed · P95', f'{delay/1000:.2f} s' if delay is not None and 'OFFLINE' not in mode else '—')
        st.caption('Collector: received_at − sent_at. Processed: first_processed_at − sent_at; chưa gồm render trình duyệt. Sequence gap gồm bản tin cố ý bỏ và JSON lỗi, không đồng nghĩa mất gói mạng.')
        st.subheader('Sự kiện toàn hệ thống')
        st.caption('Bộ đếm dưới đây gồm mọi thiết bị và mọi phiên, không chỉ bộ lọc hiện tại.')
        events = storage.event_counts()
        if events:
            st.dataframe(pd.DataFrame(events.items(), columns=['Sự kiện', 'Số lượng']), hide_index=True, use_container_width=True)
        else:
            st.caption('Chưa có sự kiện.')
    with records:
        st.subheader('Dữ liệu của phiên đã chọn')
        downloads = st.columns(2)
        downloads[0].download_button('Tải raw CSV', r.drop(columns='time').to_csv(index=False), 'raw.csv', 'text/csv', use_container_width=True)
        if not p.empty:
            downloads[1].download_button('Tải processed CSV', p.drop(columns='time').to_csv(index=False), 'processed.csv', 'text/csv', use_container_width=True)
        st.caption('Xem nhanh 50 bản tin gần nhất. CSV chứa toàn bộ phiên đã chọn.')
        st.dataframe(r.drop(columns='time').tail(50), hide_index=True, use_container_width=True)
        with st.expander('Dữ liệu sau xử lý · 50 bin gần nhất'):
            if not p.empty:
                st.dataframe(p.drop(columns='time').tail(50), hide_index=True, use_container_width=True)


dashboard()
