import streamlit as st
import pandas as pd
from datetime import datetime, date
import hashlib
import os
import uuid
import random
import plotly.express as px
import plotly.graph_objects as go

# ===================== PAGE CONFIG =====================
st.set_page_config(
    page_title="Lead Connect System",
    page_icon="💼",
    layout="wide"
)

# ===================== PATHS & STORAGE =====================
# ===================== PATHS & STORAGE =====================
DATA_FOLDER = 'data'
os.makedirs(DATA_FOLDER, exist_ok=True)

USERS_FILE      = os.path.join(DATA_FOLDER, 'users.xlsx')
CAMPAIGNS_FILE  = os.path.join(DATA_FOLDER, 'campaigns.xlsx')
ACTION_LOG_FILE = os.path.join(DATA_FOLDER, 'action_logs.csv')

# ✅ เก็บ Leads แยกไฟล์ต่อแคมเปญ
LEADS_FOLDER   = os.path.join(DATA_FOLDER, 'leads')
os.makedirs(LEADS_FOLDER, exist_ok=True)

# (รองรับไฟล์รวมเก่า ระหว่างทำ migration)
OLD_LEADS_FILE  = os.path.join(DATA_FOLDER, 'leads.xlsx')


# ===================== BASIC HELPERS =====================
def load_data(file_path: str, default_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Load Excel file into DataFrame; return default if missing."""
    if os.path.exists(file_path):
        try:
            return pd.read_excel(file_path)
        except Exception:
            pass
    return default_df.copy() if default_df is not None else pd.DataFrame()


def save_data(df: pd.DataFrame, file_path: str, sheet_name: str = 'Sheet1') -> None:
    """Save DataFrame to Excel file (xlsx)."""
    # Ensure parent folder exists
    os.makedirs(os.path.dirname(file_path) or '.', exist_ok=True)
    df.to_excel(file_path, sheet_name=sheet_name, index=False)


def append_to_csv(data_dict: dict, file_path: str) -> None:
    """Append a single-row dict to CSV (for action logs)."""
    df = pd.DataFrame([data_dict])
    mode = 'a' if os.path.exists(file_path) else 'w'
    header = not os.path.exists(file_path)
    df.to_csv(file_path, mode=mode, header=header, index=False, encoding='utf-8-sig')


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def log_action(user_id: str, action_type: str, table_name: str, record_id: str,
               old_values: dict | None = None, new_values: dict | None = None) -> None:
    """Write an action log row to CSV."""
    log_data = {
        'log_id': str(uuid.uuid4()),
        'user_id': user_id,
        'action_type': action_type,
        'table_name': table_name,
        'record_id': record_id,
        'old_values': str(old_values) if old_values is not None else None,
        'new_values': str(new_values) if new_values is not None else None,
        'action_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    append_to_csv(log_data, ACTION_LOG_FILE)

# ===================== LEADS (SPLIT FILES) HELPERS =====================
LEAD_SCHEMA_COLS = [
    'lead_id', 'campaign_id', 'customer_name', 'phone', 'email',
    'birth_date', 'investment_level', 'previous_product', 'investment_budget',
    'preferred_contact', 'policy_name', 'maturity_date', 'maturity_amount',
    'assigned_hub', 'assigned_ic', 'status', 'priority', 'last_contact_date',
    'next_contact_date', 'notes', 'created_at', 'updated_at'
]


def load_all_leads() -> pd.DataFrame:
    """Read all campaign-specific leads files and concatenate into one DataFrame."""
    frames: list[pd.DataFrame] = []

    # If folder empty but old single-file exists, load it for migration
    has_child_files = any(
        os.path.isfile(os.path.join(LEADS_FOLDER, fn)) and fn.lower().endswith(('.xlsx', '.csv'))
        for fn in os.listdir(LEADS_FOLDER)
    )
    if not has_child_files and os.path.exists(OLD_LEADS_FILE):
        try:
            frames.append(pd.read_excel(OLD_LEADS_FILE))
        except Exception:
            pass

    # Load all split files
    for fn in os.listdir(LEADS_FOLDER):
        path = os.path.join(LEADS_FOLDER, fn)
        if not os.path.isfile(path):
            continue
        try:
            if fn.lower().endswith('.xlsx'):
                frames.append(pd.read_excel(path))
            elif fn.lower().endswith('.csv'):
                frames.append(pd.read_csv(path))
        except Exception:
            continue

    if not frames:
        return pd.DataFrame(columns=LEAD_SCHEMA_COLS)

    df_all = pd.concat(frames, ignore_index=True)
    # Ensure all schema cols present
    for c in LEAD_SCHEMA_COLS:
        if c not in df_all.columns:
            df_all[c] = None
    return df_all[LEAD_SCHEMA_COLS]


def save_leads_for_campaign(df_campaign: pd.DataFrame, campaign_id: str) -> None:
    """Write a campaign's leads DataFrame to its own Excel file."""
    fname = f"leads_{campaign_id}.xlsx"
    fpath = os.path.join(LEADS_FOLDER, fname)
    save_data(df_campaign, fpath)


def cleanup_stale_lead_files(valid_campaign_ids: set[str]) -> None:
    """Remove leads files that do not belong to any current campaign id."""
    for fn in os.listdir(LEADS_FOLDER):
        if not fn.startswith('leads_'):
            continue
        if not fn.lower().endswith(('.xlsx', '.csv')):
            continue
        cid = os.path.splitext(fn)[0].replace('leads_', '')
        if cid not in valid_campaign_ids:
            try:
                os.remove(os.path.join(LEADS_FOLDER, fn))
            except Exception:
                pass

# ===================== LOAD/SAVE ALL =====================
def load_all_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Load users, campaigns, and merged leads from split files."""
    users_df = load_data(USERS_FILE, pd.DataFrame(columns=[
        'user_id', 'username', 'password_hash', 'full_name', 'role', 'hub_name', 'created_at'
    ]))

    # campaigns_df = load_data(CAMPAIGNS_FILE, pd.DataFrame(columns=[
    #     'campaign_id', 'campaign_name', 'campaign_type', 'description',
    #     'start_date', 'end_date', 'target_amount', 'image_path',
    #     'document_path', 'created_by', 'created_at', 'status'
    # ]))
    campaigns_df = load_data(CAMPAIGNS_FILE, pd.DataFrame(columns=[
        'campaign_id', 'campaign_name', 'campaign_type', 'description',
        'start_date', 'end_date', 'image_path',
        'document_path', 'created_by', 'created_at', 'status'
    ]))

    leads_df = load_all_leads()
    return users_df, campaigns_df, leads_df


def save_all_data(users_df: pd.DataFrame, campaigns_df: pd.DataFrame, leads_df: pd.DataFrame | None) -> None:
    """Save users/campaigns; write leads split by campaign, and clean stale files."""
    save_data(users_df, USERS_FILE)
    save_data(campaigns_df, CAMPAIGNS_FILE)

    if leads_df is None or leads_df.empty:
        cleanup_stale_lead_files(set())
        return

    for cid, group in leads_df.groupby('campaign_id'):
        if pd.isna(cid):
            continue
        save_leads_for_campaign(group, str(cid))

    valid_ids = set(leads_df['campaign_id'].dropna().astype(str).unique().tolist())
    cleanup_stale_lead_files(valid_ids)

# ===================== ID GENERATION =====================
def generate_campaign_id() -> str:
    """Generate next campaign id as CAMP-XXX from existing file."""
    _, campaigns_df, _ = load_all_data()
    existing_ids = campaigns_df['campaign_id'].astype(str) if not campaigns_df.empty else []
    numbers: list[int] = []
    for cid in existing_ids:
        if cid.startswith('CAMP-'):
            try:
                numbers.append(int(cid.split('-')[1]))
            except Exception:
                pass
    next_num = (max(numbers) + 1) if numbers else 1
    return f"CAMP-{next_num:03d}"

def render_multiline(label: str, text: str | None):
    """แสดงข้อความหลายบรรทัดแบบคงบรรทัดตามที่กรอก"""
    import html
    s = "" if text is None else str(text)
    s = s.replace("\r\n", "\n").replace("\r", "\n").strip()
    safe = html.escape(s).replace("\n", "<br/>")
    st.markdown(f"**{label}**<br/>{safe}", unsafe_allow_html=True)

# ===================== AUTH =====================
def login_page():
    st.title("🔐 Lead Connect")
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")
        if st.button("Login", use_container_width=True):
            users_df, _, _ = load_all_data()
            user = users_df[users_df['username'] == username]
            if not user.empty and user.iloc[0]['password_hash'] == hash_password(password):
                st.session_state['user'] = {
                    'user_id': user.iloc[0]['user_id'],
                    'username': user.iloc[0]['username'],
                    'full_name': user.iloc[0]['full_name'],
                    'role': user.iloc[0]['role'],
                    'hub': user.iloc[0]['hub_name']
                }
                st.rerun()
            else:
                st.error("Username or password is incorrect")
        #st.info("**Demo Accounts:**\n- Admin: admin / admin123\n- IC: ic101 / password1\n- IC Team2: ic201 / password4")

# ===================== IC DASHBOARD =====================
def ic_dashboard(user: dict):
    st.title(f"📊 Dashboard - {user['full_name']}")
    _, _, leads_df = load_all_data()

    my_leads = leads_df[leads_df['assigned_ic'] == user['username']]

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("จำนวน Lead ทั้งหมด", len(my_leads))
    col2.metric("ปิดการขายสำเร็จ", (my_leads['status'] == 'ปิดการขายสำเร็จ').sum())
    col3.metric("ยังไม่ติดต่อ", (my_leads['status'] == 'ยังไม่ติดต่อ').sum())
    col4.metric("High Priority", (my_leads['priority'] == 'High').sum())  # fixed

    st.subheader("📈 สถิติราย Campaign")
    _, campaigns_df, _ = load_all_data()
    if not my_leads.empty:
        stats_df = my_leads.groupby('campaign_id').agg(
            total=('lead_id', 'count'),
            closed=('status', lambda x: (x == 'ปิดการขายสำเร็จ').sum())
        ).reset_index()
        stats_df['pending'] = stats_df['total'] - stats_df['closed']
        stats_df = stats_df.merge(campaigns_df[['campaign_id', 'campaign_name']], on='campaign_id', how='left')
        fig = px.bar(stats_df, x='campaign_name', y=['closed', 'pending'],
                     title='สถานะการติดต่อราย Campaign', labels={'value': 'จำนวน', 'variable': 'สถานะ'})
        st.plotly_chart(fig, use_container_width=True)

# ===================== IC CAMPAIGN DETAIL =====================
# def campaign_detail_ic(user: dict, campaign_id: str):
#     _, campaigns_df, leads_df = load_all_data()
#     campaign = campaigns_df[campaigns_df['campaign_id'] == campaign_id].iloc[0]

#     st.title(f"📋 {campaign['campaign_name']}")
#     c1, c2 = st.columns([2, 1])
#     with c1:
#         st.write(f"**ประเภท:** {campaign['campaign_type']}")
#         
#         st.write(f"**:** {campaign['start_date']} ถึง {campaign['end_date']}")
#         st.write(f"**เป้าหมาย:** {campaign['target_amount']:,.0f} บาท")

#     # Leads table for this IC
#     st.subheader("รายชื่อ Lead")
#     # my_leads = leads_df[(leads_df['campaign_id'] == campaign_id) & (leads_df['assigned_ic'] == user['username'])].copy()

#     # if my_leads.empty:
#     #     st.info("ยังไม่มี Lead ใน Campaign นี้")
#     #     return

#     # display_df = my_leads[['customer_name', 'phone', 'email', 'policy_name', 'maturity_date', 'maturity_amount', 'status', 'priority', 'notes']].copy()
    
#     my_leads = leads_df[(leads_df['campaign_id'] == campaign_id) & (leads_df['assigned_ic'] == user['username'])].copy()

#     # ✅ บังคับ dtype ให้เข้ากันกับคอลัมน์แบบ TextColumn
#     text_cols = ['customer_name', 'phone', 'email', 'policy_name', 'maturity_date', 'notes']
#     for c in text_cols:
#         if c in my_leads.columns:
#             my_leads[c] = my_leads[c].fillna('').astype(str)

#     # (ทางเลือก) ให้คอลัมน์ตัวเลขแน่ใจว่าเป็นตัวเลข เพื่อใช้ NumberColumn ได้
#     if 'maturity_amount' in my_leads.columns:
#         my_leads['maturity_amount'] = pd.to_numeric(my_leads['maturity_amount'], errors='coerce')

#     # (ทางเลือก) เติมค่าตั้งต้นให้ status/priority ถ้าว่าง เพื่อให้ selectbox แสดงได้ดี
#     if 'status' in my_leads.columns:
#         my_leads['status'] = my_leads['status'].fillna('ยังไม่ติดต่อ')
#     if 'priority' in my_leads.columns:
#         my_leads['priority'] = my_leads['priority'].fillna('Medium')

#     # จากนั้นค่อยสร้าง display_df ต่อได้เลย
#     display_df = my_leads[['customer_name', 'phone', 'email', 'policy_name',
#                         'maturity_date', 'maturity_amount', 'status',
#                         'priority', 'notes']].copy()

#     edited_df = st.data_editor(
#         display_df,
#         column_config={
#             'customer_name': st.column_config.TextColumn('ชื่อลูกค้า', disabled=True),
#             'phone': st.column_config.TextColumn('เบอร์โทร', disabled=True),
#             'email': st.column_config.TextColumn('อีเมล', disabled=True),
#             'policy_name': st.column_config.TextColumn('ชื่อกรมธรรม์', disabled=True),
#             'maturity_date': st.column_config.TextColumn('วันครบกำหนด', disabled=True),
#             'maturity_amount': st.column_config.NumberColumn('จำนวนเงิน', format='%.0f', disabled=True),
#             'status': st.column_config.SelectboxColumn('สถานะ', options=['ยังไม่ติดต่อ','ติดต่อแล้ว','ปิดการขายสำเร็จ','รอตัดสินใจ','ไม่สนใจ','ติดต่อไม่ได้']),
#             'priority': st.column_config.SelectboxColumn('Priority', options=['High','Medium','Low']),
#             'notes': st.column_config.TextColumn('หมายเหตุ')
#         },
#         use_container_width=True,
#         height=420
#     )

#     if st.button("บันทึกการเปลี่ยนแปลง", type="primary"):
#         users_df, campaigns_df, leads_all = load_all_data()
#         for idx, (lead_id, orig) in enumerate(zip(my_leads['lead_id'], my_leads.itertuples())):
#             new_status = edited_df.iloc[idx]['status']
#             new_priority = edited_df.iloc[idx]['priority']
#             new_notes = edited_df.iloc[idx]['notes']
#             if (new_status != orig.status) or (new_priority != orig.priority) or (new_notes != str(orig.notes or '')):
#                 mask = leads_all['lead_id'] == lead_id
#                 leads_all.loc[mask, 'status'] = new_status
#                 leads_all.loc[mask, 'priority'] = new_priority
#                 leads_all.loc[mask, 'notes'] = (new_notes or None) #new_notes
#                 leads_all.loc[mask, 'updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#                 leads_all.loc[mask, 'last_contact_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
#         save_all_data(users_df, campaigns_df, leads_all)
#         st.success("บันทึกการเปลี่ยนแปลงสำเร็จ!")
#         #st.rerun()

#     st.subheader("ดาวน์โหลดรายชื่อ Lead")
#     csv = my_leads.to_csv(index=False, encoding='utf-8-sig')
#     st.download_button(label=f"ดาวน์โหลด {campaign['campaign_name']}", data=csv, file_name=f"leads_{campaign['campaign_name']}_{datetime.now().strftime('%Y%m%d')}.csv", mime="text/csv")

def campaign_detail_ic(user, campaign_id):
    # --- helpers ---
    def _parse_contact_date_time(dt_str):
        """Split last_contact_date 'YYYY-mm-dd HH:MM:SS' -> (date|None, time|None)"""
        if pd.isna(dt_str) or not dt_str:
            return None, None
        try:
            ts = pd.to_datetime(dt_str)
            return ts.date(), ts.time().replace(microsecond=0)
        except Exception:
            return None, None

    def _to_date(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        if isinstance(val, pd.Timestamp):
            return val.date()
        if isinstance(val, datetime):
            return val.date()
        if isinstance(val, date):
            return val
        # try parse string
        try:
            return pd.to_datetime(val).date()
        except Exception:
            return None

    def _to_time(val):
        if val is None or (isinstance(val, float) and pd.isna(val)):
            return None
        if isinstance(val, pd.Timestamp):
            return val.time().replace(microsecond=0)
        # datetime.time OK
        try:
            import datetime as _dt
            if isinstance(val, _dt.time):
                return val.replace(microsecond=0)
        except Exception:
            pass
        # parse string "HH:MM[:SS]"
        try:
            s = str(val)
            parts = s.split(":")
            h = int(parts[0]); m = int(parts[1]) if len(parts) > 1 else 0; sec = int(parts[2]) if len(parts) > 2 else 0
            import datetime as _dt
            return _dt.time(hour=h, minute=m, second=sec)
        except Exception:
            return None

    PRIORITY_EMOJI = {"High": "🔴", "Medium": "🟡", "Low": "🟢"}
    STATUS_OPTIONS = ['ยังไม่ติดต่อ', 'ติดต่อแล้ว', 'ปิดการขายสำเร็จ', 'รอตัดสินใจ', 'ไม่สนใจ', 'ติดต่อไม่ได้']
    STATUS_EMOJI = {
        'ยังไม่ติดต่อ': '⚪️',
        'ติดต่อแล้ว': '🟦',
        'ปิดการขายสำเร็จ': '🟩',
        'รอตัดสินใจ': '🟨',
        'ไม่สนใจ': '🟥',
        'ติดต่อไม่ได้': '🟪',
    }

    # --- load campaign ---
    _, campaigns_df, leads_df = load_all_data()
    campaign = campaigns_df[campaigns_df['campaign_id'] == campaign_id].iloc[0]

    st.title(f"📋 {campaign['campaign_name']}")

    colA, colB = st.columns([2, 1])
    with colA:
        st.write(f"**ประเภท:** {campaign['campaign_type']}")
        # st.write(f"**รายละเอียด:** {campaign['description']}")
        render_multiline("รายละเอียด:", campaign["description"])
        st.write(f"**ระยะเวลาติดต่อลูกค้า:** {campaign['start_date']} ถึง {campaign['end_date']}")
        #st.write(f"**เป้าหมาย:** {campaign['target_amount']:,.0f} บาท")

    # --- filter controls ---
    st.markdown("### 🔎 ตัวกรอง")
    f1, f2 = st.columns(2)
    with f1:
        priority_filter = st.selectbox("Priority", ["ทั้งหมด", "High", "Medium", "Low"], index=0)
    with f2:
        status_filter = st.selectbox("สถานะการติดต่อ", ["ทั้งหมด"] + STATUS_OPTIONS, index=0)

    # --- my leads for this campaign ---
    my_leads = leads_df[
        (leads_df['campaign_id'] == campaign_id) &
        (leads_df['assigned_ic'] == user['username'])
    ].copy()

    if my_leads.empty:
        st.info("ยังไม่มี Lead ใน Campaign นี้")
        return

    # apply filters
    if priority_filter != "ทั้งหมด":
        my_leads = my_leads[my_leads['priority'] == priority_filter]
    if status_filter != "ทั้งหมด":
        my_leads = my_leads[my_leads['status'] == status_filter]

    if my_leads.empty:
        st.warning("ไม่พบข้อมูลตามตัวกรองที่เลือก")
        return

    # --- dtype/prepare fields ---
    # text-like columns → strings
    text_cols = ['customer_name', 'phone', 'email', 'policy_name', 'maturity_date', 'notes', 'status', 'priority']
    for c in text_cols:
        if c in my_leads.columns:
            my_leads[c] = my_leads[c].fillna('').astype(str)

    # numeric
    if 'maturity_amount' in my_leads.columns:
        my_leads['maturity_amount'] = pd.to_numeric(my_leads['maturity_amount'], errors='coerce')

    # default fallback
    my_leads['status'] = my_leads['status'].replace('', 'ยังไม่ติดต่อ')
    my_leads['priority'] = my_leads['priority'].replace('', 'Medium')

    # contact date/time columns split from last_contact_date
    contact_dates = []
    contact_times = []
    for v in my_leads['last_contact_date'].tolist():
        d, t = _parse_contact_date_time(v)
        contact_dates.append(d)
        contact_times.append(t)
    my_leads['contact_date'] = contact_dates
    my_leads['contact_time'] = contact_times

    # customer code (อ่านง่าย): ใช้ท้าย 8 ตัวของ lead_id
    my_leads['customer_code'] = my_leads['lead_id'].astype(str).str[-8:].str.upper()

    # Priority display with color
    my_leads['priority_display'] = my_leads['priority'].apply(
        lambda p: f"{p} {PRIORITY_EMOJI.get(p, '')}"
    )
    # Status colored label (read-only)
    my_leads['status_label'] = my_leads['status'].apply(
        lambda s: f"{STATUS_EMOJI.get(s, '')} {s}"
    )

    # --- choose columns by campaign type ---
    is_ipo = str(campaign['campaign_type']).strip().upper() == 'IPO'

    if is_ipo:
        # IPO columns
        cols = [
            'customer_code', 'customer_name', 'phone', 'email',
            'priority_display',      # readonly
            'status',                # editable dropdown
            'contact_date',          # editable date
            'contact_time',          # editable time
            'notes'                  # editable text
        ]
    else:
        # Insurance/Bond/Other columns
        cols = [
            'customer_code', 'customer_name', 'phone', 'email',
            'policy_name', 'maturity_date', 'maturity_amount',
            'priority_display',      # readonly
            'status',                # editable dropdown
            'contact_date',          # editable date
            'contact_time',          # editable time
            'notes'                  # editable text
        ]

    display_df = my_leads[cols + ['lead_id', 'priority']].copy()  # keep lead_id/priority for saving
    display_df = display_df.set_index('lead_id')  # use lead_id as stable index in editor

    # --- data editor config ---
    column_config = {
        'customer_code': st.column_config.TextColumn("รหัสลูกค้า", disabled=True),
        'customer_name': st.column_config.TextColumn("ชื่อลูกค้า", disabled=True),
        'phone': st.column_config.TextColumn("เบอร์โทร", disabled=True),
        'email': st.column_config.TextColumn("อีเมล", disabled=True),
        'priority_display': st.column_config.TextColumn("Priority", disabled=True),
        'status': st.column_config.SelectboxColumn("สถานะการติดต่อ", options=STATUS_OPTIONS),
        'contact_date': st.column_config.DateColumn("วันที่ติดต่อ"),
        'contact_time': st.column_config.TimeColumn("เวลาที่ติดต่อ"),
        'notes': st.column_config.TextColumn("หมายเหตุ"),
    }
    if not is_ipo:
        column_config.update({
            'policy_name': st.column_config.TextColumn("Product Name", disabled=True),
            'maturity_date': st.column_config.TextColumn("วันที่ครบกำหนด", disabled=True),
            'maturity_amount': st.column_config.NumberColumn("จำนวนเงิน", format="%.0f", disabled=True),
        })

    # hide helper col
    column_order = [c for c in cols if c in display_df.columns]

    st.markdown("### รายชื่อ Lead")
    edited_df = st.data_editor(
        display_df[column_order],
        column_config=column_config,
        use_container_width=True,
        hide_index=True,  # show lead_id? We set index to lead_id; but they asked 'รหัสลูกค้า' so index can be hidden.
        num_rows="fixed",
        height=520
    )

    # --- save button ---
    # if st.button("บันทึกการเปลี่ยนแปลง", type="primary"):
    #     users_df, campaigns_df, all_leads = load_all_data()

    #     # loop through edited rows by index (lead_id)
    #     changes = 0
    #     for lead_id, row in edited_df.iterrows():
    #         # get original row mask
    #         mask = all_leads['lead_id'] == lead_id
    #         if not mask.any():
    #             continue

    #         # read new values
    #         new_status = row.get('status')
    #         new_notes = row.get('notes')
    #         new_date = _to_date(row.get('contact_date'))
    #         new_time = _to_time(row.get('contact_time'))

    #         # combine date+time to last_contact_date string
    #         if new_date is not None:
    #             if new_time is not None:
    #                 last_contact_str = f"{new_date.strftime('%Y-%m-%d')} {new_time.strftime('%H:%M:%S')}"
    #             else:
    #                 last_contact_str = f"{new_date.strftime('%Y-%m-%d')} 00:00:00"
    #         else:
    #             # ถ้าไม่ใส่วัน ให้เว้น last_contact_date ไว้ตามเดิม
    #             last_contact_str = all_leads.loc[mask, 'last_contact_date'].iloc[0]

    #         # check diffs
    #         cur_status = str(all_leads.loc[mask, 'status'].iloc[0] or '')
    #         cur_notes = str(all_leads.loc[mask, 'notes'].iloc[0] or '')
    #         cur_last_contact = all_leads.loc[mask, 'last_contact_date'].iloc[0]

    #         if (new_status != cur_status) or (str(new_notes or '') != cur_notes) or (last_contact_str != cur_last_contact):
    #             all_leads.loc[mask, 'status'] = new_status
    #             all_leads.loc[mask, 'notes'] = (new_notes or None)
    #             all_leads.loc[mask, 'last_contact_date'] = last_contact_str
    #             all_leads.loc[mask, 'updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    #             changes += 1

    #     save_all_data(users_df, campaigns_df, all_leads)
    #     if changes:
    #         st.success(f"บันทึกการเปลี่ยนแปลงสำเร็จ ({changes} รายการ)")
    #     else:
    #         st.info("ไม่มีการเปลี่ยนแปลง")
    #     st.rerun()
        # --- save button (with validation rules) ---
    if st.button("บันทึกการเปลี่ยนแปลง", type="primary"):
        users_df, campaigns_df, all_leads = load_all_data()

        # กฎสถานะ
        requires_contact = {'ติดต่อแล้ว', 'ปิดการขายสำเร็จ', 'รอตัดสินใจ', 'ไม่สนใจ', 'ติดต่อไม่ได้'}
        no_contact = {'ยังไม่ติดต่อ'}

        invalid_required = []   # แถวที่ "ต้องมี" วัน/เวลา แต่ขาด
        invalid_forbidden = []  # แถวที่ "ห้ามมี" วัน/เวลา แต่ดันมี
        changes = 0

        # ใช้ข้อมูลเพื่อรายงานชื่อ/รหัสลูกค้าให้อ่านง่าย
        # (แกะจาก edited_df ซึ่งมี 'customer_code' อยู่แล้ว)
        def _row_label(lead_id):
            try:
                code = edited_df.loc[lead_id].get('customer_code', '')
                name = edited_df.loc[lead_id].get('customer_name', '')
                return f"{code} - {name}".strip(" -")
            except Exception:
                return str(lead_id)

        # 1) ตรวจ validation ทุกแถวก่อน — ถ้ามีผิดจะไม่บันทึกเลย
        for lead_id, row in edited_df.iterrows():
            new_status = str(row.get('status') or '').strip()
            d = _to_date(row.get('contact_date'))
            t = _to_time(row.get('contact_time'))

            if new_status in requires_contact:
                if d is None or t is None: 
                    invalid_required.append(_row_label(lead_id))

            if new_status in no_contact:
                # ห้ามมีทั้งวันและเวลา (มีอย่างใดอย่างหนึ่งก็ถือว่าผิด)
                if (d is not None) or (t is not None):
                    invalid_forbidden.append(_row_label(lead_id))

        if invalid_required or invalid_forbidden:
            if invalid_required:
                st.error("ต้องระบุ 'วันที่ติดต่อ' และ 'เวลาที่ติดต่อ' สำหรับสถานะ: ติดต่อแล้ว/ปิดการขายสำเร็จ/รอตัดสินใจ/ไม่สนใจ/ติดต่อไม่ได้\n"
                         + "\n• " + "\n\n• ".join(invalid_required))
            if invalid_forbidden:
                st.error("สถานะ 'ยังไม่ติดต่อ' ห้ามมี 'วันที่ติดต่อ' และ 'เวลาที่ติดต่อ'\n"
                         + "\n• " + "\n\n• ".join(invalid_forbidden))
            st.stop()  # ยุติการบันทึก

        # 2) ผ่าน validation แล้ว — ดำเนินการบันทึก
        for lead_id, row in edited_df.iterrows():
            mask = all_leads['lead_id'] == lead_id
            if not mask.any():
                continue

            new_status = str(row.get('status') or '').strip()
            new_notes  = row.get('notes')

            d = _to_date(row.get('contact_date'))
            t = _to_time(row.get('contact_time'))

            # รวมวัน+เวลาเพื่อเก็บใน last_contact_date ตามกฎ
            if new_status in requires_contact:
                # (ถึงตรงนี้ d/t ต้องไม่ None แล้ว เพราะผ่าน validation)
                last_contact_str = f"{d.strftime('%Y-%m-%d')} {t.strftime('%H:%M:%S')}"
            elif new_status in no_contact:
                last_contact_str = None  # ต้องไม่มี
            else:
                # เผื่ออนาคตมีสถานะอื่น ๆ — หากไม่กำหนด ก็คงค่าเดิมไว้
                last_contact_str = all_leads.loc[mask, 'last_contact_date'].iloc[0]

            cur_status = str(all_leads.loc[mask, 'status'].iloc[0] or '')
            cur_notes  = str(all_leads.loc[mask, 'notes'].iloc[0] or '')
            cur_last   = all_leads.loc[mask, 'last_contact_date'].iloc[0]

            if (new_status != cur_status) or (str(new_notes or '') != cur_notes) or (last_contact_str != cur_last):
                all_leads.loc[mask, 'status'] = new_status
                all_leads.loc[mask, 'notes']  = (new_notes or None)
                all_leads.loc[mask, 'last_contact_date'] = last_contact_str
                all_leads.loc[mask, 'updated_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                changes += 1

        save_all_data(users_df, campaigns_df, all_leads)
        if changes:
            st.success(f"บันทึกการเปลี่ยนแปลงสำเร็จ ({changes} รายการ)")
        else:
            st.info("ไม่มีการเปลี่ยนแปลง")
        # st.rerun()




# ===================== ADMIN DASHBOARD =====================
def admin_dashboard(user: dict):
    st.title("📊 Admin Dashboard")
    users_df, campaigns_df, leads_df = load_all_data()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Lead ทั้งหมด", len(leads_df))
    col2.metric("Campaign ทั้งหมด", len(campaigns_df))
    col3.metric("IC ทั้งหมด", (users_df['role'] == 'ic').sum())
    col4.metric("ปิดการขายสำเร็จ", (leads_df['status'] == 'ปิดการขายสำเร็จ').sum())

    st.subheader("สถิติราย Hub")
    if not leads_df.empty:
        hub_stats = leads_df.groupby('assigned_hub').agg(
            total=('lead_id', 'count'),
            closed=('status', lambda x: (x == 'ปิดการขายสำเร็จ').sum())
        ).reset_index()
        hub_stats['pending'] = hub_stats['total'] - hub_stats['closed']
        fig = px.bar(hub_stats, x='assigned_hub', y=['closed', 'pending'], title='สถานะการติดต่อราย Hub', labels={'value':'จำนวน','variable':'สถานะ'})
        st.plotly_chart(fig, use_container_width=True)

# ===================== ADD CAMPAIGN STATE HELPERS =====================
def init_add_campaign_state():
    if 'add_campaign_success' not in st.session_state:
        st.session_state['add_campaign_success'] = False
    if 'df_preview' not in st.session_state:
        st.session_state['df_preview'] = None
    if 'uploader_keys' not in st.session_state:
        st.session_state['uploader_keys'] = {'leads': 0, 'img': 0, 'doc': 0}
    if 'last_campaign_id' not in st.session_state:
        st.session_state['last_campaign_id'] = None
    if 'last_campaign_name' not in st.session_state:
        st.session_state['last_campaign_name'] = None
    if 'df_uploaded_once' not in st.session_state:
        st.session_state['df_uploaded_once'] = False


def clear_add_campaign_form():
    # Clear inputs but keep success banner & last campaign info by default
    for k in ['campaign_name','campaign_type','description','start_date','end_date','target_amount']:
        if k in st.session_state:
            del st.session_state[k]
    st.session_state['df_preview'] = None
    st.session_state['df_uploaded_once'] = False
    st.session_state['uploader_keys']['leads'] += 1
    st.session_state['uploader_keys']['img']   += 1
    st.session_state['uploader_keys']['doc']   += 1


def start_new_campaign():
    # Hide success, reset everything
    clear_add_campaign_form()
    st.session_state['last_campaign_id'] = None
    st.session_state['last_campaign_name'] = None
    st.session_state['add_campaign_success'] = False

# ===================== ADMIN: MANAGE CAMPAIGNS =====================
def manage_campaigns_admin(user: dict):
    st.title("จัดการ Campaign")

    init_add_campaign_state()
    tab1, tab2 = st.tabs(["เพิ่ม Campaign", "จัดการ Campaign"]) 

    # ---------- TAB 1: ADD CAMPAIGN ----------
    with tab1:
        st.subheader("เพิ่ม Campaign ใหม่")
        next_id = generate_campaign_id()
        st.info(f"📋 Campaign ID ต่อไปที่จะใช้: **{next_id}**")

        if st.session_state['add_campaign_success']:
            cid = st.session_state.get('last_campaign_id')
            cname = st.session_state.get('last_campaign_name')
            if cid and cname:
                st.success(f"✅ บันทึกข้อมูลสำเร็จ — {cid} : {cname}")
            else:
                st.success("✅ บันทึกข้อมูลสำเร็จ")
            c1, c2 = st.columns(2)
            with c1:
                st.button("สร้าง Campaign ใหม่", type="primary", on_click=start_new_campaign, use_container_width=True)
            with c2:
                st.button("Clear all", on_click=clear_add_campaign_form, use_container_width=True)
        else:
            # ---- Campaign basic fields (not in st.form) ----
            st.text_input("ชื่อ Campaign*", key="campaign_name")
            st.selectbox("ประเภท*", ['IPO','Insurance','Bond','Other'], key="campaign_type")
            st.text_area("รายละเอียด*", key="description")
            st.date_input("วันที่เริ่มการติดต่อลูกค้า*", key="start_date")
            st.date_input("วันที่สิ้นสุดการติดต่อลูกค้า*", key="end_date")
            #st.number_input("เป้าหมายยอดขาย", min_value=0, key="target_amount")

            uploaded_image = st.file_uploader("อัพโหลดรูปภาพ", type=['png','jpg','jpeg'], key=f"img_uploader_{st.session_state['uploader_keys']['img']}")
            uploaded_doc   = st.file_uploader("อัพโหลดเอกสาร", type=['pdf'], key=f"doc_uploader_{st.session_state['uploader_keys']['doc']}")

            # ---- Leads upload (below fields) ----
            st.markdown("### อัพโหลดรายชื่อ Lead*")
            uploaded_leads = st.file_uploader("เลือกไฟล์ Lead (CSV/Excel)", type=['csv','xlsx'], key=f"leads_uploader_{st.session_state['uploader_keys']['leads']}")

            if uploaded_leads is not None:
                try:
                    uploaded_leads.seek(0)
                    if uploaded_leads.name.lower().endswith('.csv'):
                        st.session_state['df_preview'] = pd.read_csv(uploaded_leads)
                    else:
                        st.session_state['df_preview'] = pd.read_excel(uploaded_leads)
                    st.session_state['df_uploaded_once'] = True
                except Exception as e:
                    st.session_state['df_preview'] = None
                    st.session_state['df_uploaded_once'] = False
                    st.error(f"ไม่สามารถอ่านไฟล์ได้: {e}")

            has_df = st.session_state['df_preview'] is not None
            has_assigned_ic = has_df and ('assigned_ic' in st.session_state['df_preview'].columns)

            # Validate campaign_id column equals next_id
            campaign_col_ok = has_df and ('campaign_id' in st.session_state['df_preview'].columns)
            campaign_id_match = False
            if has_df:
                if not campaign_col_ok:
                    st.error("❌ ไฟล์ต้องมีคอลัมน์ `campaign_id` และค่าต้องตรงกับ Campaign ID ที่กำลังสร้าง")
                else:
                    expected = str(next_id)
                    col = st.session_state['df_preview']['campaign_id'].astype(str).str.strip()
                    bad_rows = col[col != expected]
                    campaign_id_match = bad_rows.empty
                    if campaign_id_match:
                        st.success(f"✅ `campaign_id` ในไฟล์ตรงกับ `{expected}`")
                    else:
                        st.error(f"❌ `campaign_id` ต้องเป็น `{expected}` ทั้งหมด พบค่าที่ไม่ตรง {bad_rows.nunique()} แบบ")

            # Preview
            if has_df:
                st.write("📋 พรีวิวข้อมูลที่จะนำเข้า:")
                st.dataframe(st.session_state['df_preview'].head())
                if not has_assigned_ic:
                    st.error("❌ ไฟล์ต้องมีคอลัมน์ `assigned_ic`")
            else:
                st.info("โปรดอัพโหลดไฟล์ Lead (ต้องมีคอลัมน์ assigned_ic และ campaign_id)")

            st.button("Clear all", on_click=clear_add_campaign_form)

            # Date validation
            start_val = st.session_state.get('start_date')
            end_val   = st.session_state.get('end_date')
            date_ok   = (start_val is not None) and (end_val is not None) and (end_val >= start_val)
            if start_val and end_val and not date_ok:
                st.error("❌ วันที่สิ้นสุดต้องไม่น้อยกว่าวันที่เริ่ม")

            fields_ok = bool(st.session_state.get('campaign_name')) \
                        and bool(st.session_state.get('campaign_type')) \
                        and bool(st.session_state.get('description')) \
                        and (start_val is not None) and (end_val is not None)

            ready_to_save = fields_ok and date_ok and has_assigned_ic and campaign_id_match

            if st.button("บันทึก Campaign และ Lead", type="primary", disabled=not ready_to_save):
                users_df, campaigns_df, leads_df = load_all_data()

                # Save campaign
                new_campaign = pd.DataFrame([{
                    'campaign_id': next_id,
                    'campaign_name': st.session_state['campaign_name'],
                    'campaign_type': st.session_state['campaign_type'],
                    'description': st.session_state['description'],
                    'start_date': start_val.strftime('%Y-%m-%d'),
                    'end_date': end_val.strftime('%Y-%m-%d'),
                    #'target_amount': st.session_state.get('target_amount', 0),
                    'image_path': None,
                    'document_path': None,
                    'created_by': user['user_id'],
                    'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'status': 'Active'
                }])
                campaigns_df = pd.concat([campaigns_df, new_campaign], ignore_index=True)

                # Save leads (force campaign_id to next_id)
                dfp = st.session_state['df_preview']
                success, missing_ic = 0, []
                for _, row in dfp.iterrows():
                    ic_username = row.get('assigned_ic')
                    if ic_username not in users_df['username'].values:
                        missing_ic.append(ic_username)
                        continue
                    new_lead = pd.DataFrame([{
                        'lead_id': str(uuid.uuid4()),
                        'campaign_id': next_id,
                        'customer_name': row.get('customer_name', ''),
                        'phone': row.get('phone', ''),
                        'email': row.get('email', ''),
                        'birth_date': row.get('birth_date'),
                        'investment_level': row.get('investment_level','Beginner'),
                        'previous_product': row.get('previous_product'),
                        'investment_budget': row.get('investment_budget'),
                        'preferred_contact': row.get('preferred_contact','Phone'),
                        'policy_name': row.get('policy_name'),
                        'maturity_date': row.get('maturity_date'),
                        'maturity_amount': row.get('maturity_amount'),
                        'assigned_hub': row.get('assigned_hub'),
                        'assigned_ic': ic_username,
                        'status': row.get('status','ยังไม่ติดต่อ'),
                        'priority': row.get('priority','Medium'),
                        'last_contact_date': None,
                        'next_contact_date': None,
                        'notes': row.get('notes'),
                        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'updated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    }])
                    leads_df = pd.concat([leads_df, new_lead], ignore_index=True)
                    success += 1

                save_all_data(users_df, campaigns_df, leads_df)

                try:
                    log_action(user_id=user['user_id'], action_type='CREATE', table_name='campaigns', record_id=next_id, new_values=new_campaign.to_dict('records')[0])
                    log_action(user_id=user['user_id'], action_type='IMPORT', table_name='leads', record_id=next_id, new_values={'imported': success})
                except Exception:
                    pass

                if missing_ic:
                    st.warning(f"⚠️ IC ต่อไปนี้ไม่พบในระบบ: {', '.join(sorted(set([str(m) for m in missing_ic if pd.notna(m)])))}")

                # Success banner showing id & name
                st.session_state['last_campaign_id'] = next_id
                st.session_state['last_campaign_name'] = st.session_state['campaign_name']
                st.session_state['add_campaign_success'] = True

                clear_add_campaign_form()  # clear inputs but keep success & last campaign info
                st.session_state['add_campaign_success'] = True
                #st.rerun()

    # ---------- TAB 2: MANAGE CAMPAIGNS ----------
    with tab2:
        st.subheader("จัดการ Campaign ที่มีอยู่")
        users_df, campaigns_df, leads_df = load_all_data()
        admin_hashes = set(users_df.loc[users_df['role'] == 'admin', 'password_hash'].dropna().astype(str).tolist())

        if campaigns_df.empty:
            st.info("ยังไม่มี Campaign")
            return

        for _, campaign in campaigns_df.iterrows():
            with st.expander(f"{campaign['campaign_id']} - {campaign['campaign_name']} ({campaign['campaign_type']})", expanded=False):
                st.write(f"**Campaign ID:** {campaign['campaign_id']}")
                # st.write(f"**รายละเอียด:** {campaign['description']}")
                render_multiline("รายละเอียด:", campaign["description"])
                st.write(f"**ระยะเวลาติดต่อลูกค้า:** {campaign['start_date']} ถึง {campaign['end_date']}")
                #st.write(f"**เป้าหมาย:** {campaign['target_amount']:,.0f} บาท")

                campaign_leads = leads_df[leads_df['campaign_id'] == campaign['campaign_id']]
                st.write(f"**จำนวน Lead:** {len(campaign_leads)} รายการ")

                if not campaign_leads.empty:
                    csv = campaign_leads.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="📥 ดาวน์โหลด Lead (CSV)",
                        data=csv,
                        file_name=f"leads_{campaign['campaign_name']}_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        key=f"dl_{campaign['campaign_id']}"
                    )

                st.markdown("---")
                with st.expander("⚙️ แก้ไขหรือลบ Campaign"):
                    pw_key = f"pw_{campaign['campaign_id']}"
                    password_input = st.text_input("กรอกรหัสผ่าน Admin เพื่อยืนยัน", type="password", key=pw_key)

                    if password_input and (hash_password(password_input) in admin_hashes):
                        st.success("✅ รหัสผ่านถูกต้อง")

                        with st.form(key=f"edit_{campaign['campaign_id']}"):
                            st.write("### แก้ไข Campaign")
                            new_name = st.text_input("ชื่อ Campaign", value=campaign['campaign_name'], key=f"nm_{campaign['campaign_id']}")
                            new_desc = st.text_area("รายละเอียด", value=campaign['description'], key=f"desc_{campaign['campaign_id']}")
                            new_start = st.date_input("วันที่เริ่ม", value=pd.to_datetime(campaign['start_date']), key=f"st_{campaign['campaign_id']}")
                            new_end   = st.date_input("วันที่สิ้นสุด", value=pd.to_datetime(campaign['end_date']), key=f"en_{campaign['campaign_id']}")
                            #new_target = st.number_input("เป้าหมาย", value=int(campaign['target_amount']), key=f"tg_{campaign['campaign_id']}")

                            if st.form_submit_button("บันทึกการแก้ไข", type="primary"):
                                idx = campaigns_df[campaigns_df['campaign_id'] == campaign['campaign_id']].index[0]
                                old_vals = campaigns_df.loc[idx].to_dict()
                                campaigns_df.at[idx, 'campaign_name'] = new_name
                                campaigns_df.at[idx, 'description']   = new_desc
                                campaigns_df.at[idx, 'start_date']    = new_start.strftime('%Y-%m-%d')
                                campaigns_df.at[idx, 'end_date']      = new_end.strftime('%Y-%m-%d')
                                #campaigns_df.at[idx, 'target_amount'] = new_target
                                save_all_data(users_df, campaigns_df, leads_df)
                                try:
                                    log_action(user_id=user['user_id'], action_type='UPDATE', table_name='campaigns', record_id=campaign['campaign_id'], old_values=old_vals, new_values=campaigns_df.loc[idx].to_dict())
                                except Exception:
                                    pass
                                st.success("✅ แก้ไข Campaign สำเร็จ")
                                st.rerun()

                        with st.form(key=f"delete_{campaign['campaign_id']}"):
                            st.write("### ลบ Campaign")
                            st.warning("⚠️ การลบ Campaign จะลบ Lead ทั้งหมดที่เชื่อมโยงด้วย")
                            confirm_name = st.text_input("พิมพ์ชื่อ Campaign เพื่อยืนยันการลบ", key=f"confirm_{campaign['campaign_id']}")
                            if st.form_submit_button("ลบ Campaign", type="secondary"):
                                if confirm_name == campaign['campaign_name']:
                                    old_campaign = campaign.to_dict()
                                    # Remove leads for this campaign
                                    leads_df = leads_df[leads_df['campaign_id'] != campaign['campaign_id']]
                                    # Remove campaign
                                    campaigns_df = campaigns_df[campaigns_df['campaign_id'] != campaign['campaign_id']]
                                    save_all_data(users_df, campaigns_df, leads_df)
                                    try:
                                        log_action(user_id=user['user_id'], action_type='DELETE', table_name='campaigns', record_id=old_campaign['campaign_id'], old_values=old_campaign)
                                        log_action(user_id=user['user_id'], action_type='DELETE', table_name='leads', record_id=old_campaign['campaign_id'], old_values={'count': int(campaign_leads.shape[0])})
                                    except Exception:
                                        pass
                                    st.success("🗑️ ลบ Campaign และ Lead สำเร็จ")
                                    st.rerun()
                                else:
                                    st.error("ชื่อ Campaign ไม่ตรงกัน")
                    elif password_input:
                        st.error("❌ รหัสผ่านไม่ถูกต้อง (ตรวจสอบให้แน่ใจว่าเป็นรหัสของผู้ใช้ role=admin)")

# ===================== MAIN APP =====================
def main():
    # Initialize data on first run
    create_mockup_data()

    if 'user' not in st.session_state:
        login_page()
        return

    user = st.session_state['user']

    # Sidebar
    with st.sidebar:
        st.title("Lead Connect")
        st.write(f"สวัสดี, {user['full_name']}")
        st.write(f"Role: {user['role']}")
        st.write(f"Hub: {user['hub']}")
        if user['role'] == 'admin':
            menu = st.selectbox("เมนู", ["Dashboard", "จัดการ Campaign", "ดู Log", "ดาวน์โหลดไฟล์"])
        else:
            menu = st.selectbox("เมนู", ["Dashboard", "Campaign"]) 
        if st.button("ออกจากระบบ"):
            del st.session_state['user']
            st.rerun()

    # Main content
    if user['role'] == 'admin':
        if menu == "Dashboard":
            admin_dashboard(user)
        elif menu == "จัดการ Campaign":
            manage_campaigns_admin(user)
        elif menu == "ดู Log":
            st.title("📋 Action Logs")
            if os.path.exists(ACTION_LOG_FILE):
                logs_df = pd.read_csv(ACTION_LOG_FILE)
                st.dataframe(logs_df, use_container_width=True)
                csv = logs_df.to_csv(index=False, encoding='utf-8-sig')
                st.download_button(label="ดาวน์โหลดไฟล์ Action Log", data=csv, file_name=f"action_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", mime="text/csv")
            else:
                st.info("ยังไม่มี Action Log")
        elif menu == "ดาวน์โหลดไฟล์":
            st.title("📥 ดาวน์โหลดไฟล์ข้อมูล")
            # Users / Campaigns / Action Logs
            files = {
                "Users": USERS_FILE,
                "Campaigns": CAMPAIGNS_FILE,
                "Action Logs": ACTION_LOG_FILE
            }
            for name, path in files.items():
                if os.path.exists(path):
                    with open(path, 'rb') as f:
                        mime_type = "text/csv" if path.endswith('.csv') else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                        st.download_button(label=f"ดาวน์โหลด {name}", data=f.read(), file_name=os.path.basename(path), mime=mime_type)
            st.markdown("### 📂 ไฟล์ Leads แยกตามแคมเปญ")
            if os.path.isdir(LEADS_FOLDER):
                lead_files = sorted([fn for fn in os.listdir(LEADS_FOLDER) if fn.lower().endswith(('.xlsx', '.csv'))])
                if not lead_files:
                    st.info("ยังไม่มีไฟล์ Leads ภายใต้แคมเปญ")
                else:
                    for fn in lead_files:
                        path = os.path.join(LEADS_FOLDER, fn)
                        with open(path, 'rb') as f:
                            mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" if fn.lower().endswith('.xlsx') else "text/csv"
                            st.download_button(label=f"ดาวน์โหลด {fn}", data=f.read(), file_name=fn, mime=mime, key=f"dl_leads_{fn}")
    else:
        if menu == "Dashboard":
            ic_dashboard(user)
        elif menu == "Campaign":
            _, campaigns_df, leads_df = load_all_data()
            my_campaign_ids = leads_df[leads_df['assigned_ic'] == user['username']]['campaign_id'].dropna().unique().tolist()
            my_campaigns = campaigns_df[campaigns_df['campaign_id'].isin(my_campaign_ids)]
            if my_campaigns.empty:
                st.info("ยังไม่มี Campaign ที่ได้รับมอบหมาย")
            else:
                selected_campaign = st.selectbox("เลือก Campaign", my_campaigns['campaign_name'].tolist())
                cid = my_campaigns[my_campaigns['campaign_name'] == selected_campaign]['campaign_id'].iloc[0]
                campaign_detail_ic(user, cid)


if __name__ == "__main__":
    main()
