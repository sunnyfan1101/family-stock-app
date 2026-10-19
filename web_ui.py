"""Web-only workspace. No report, scheduler, or database writes."""
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import streamlit as st

try:
    from supabase import create_client
    try:
        from supabase.lib.client_options import ClientOptions
    except ImportError:
        ClientOptions = None
except ImportError:  # Optional until Streamlit Secrets are configured.
    create_client = None
    ClientOptions = None


PRESETS = {
    '不限條件': {},
    '營收成長': {'revenue_growth': 20},
    '股息觀察': {'yield_rate': 3},
    '量能放大': {'vol_spike': 1.5},
}
LABELS = {
    'revenue_growth': '營收成長率 %',
    'eps_growth': 'EPS 成長率 %',
    'yield_rate': '殖利率 %',
    'position_1y': '近一年位階',
    'vol_spike': '量能倍數',
}

RESEARCH_KEYS = [
    'web_preset', 'web_industry', 'web_max_price', 'web_latest',
    'web_enable_revenue_growth', 'web_min_revenue_growth',
    'web_enable_eps_growth', 'web_min_eps_growth',
    'web_enable_yield_rate', 'web_min_yield_rate',
    'web_enable_position_1y', 'web_min_position_1y', 'web_max_position_1y',
    'web_enable_vol_spike', 'web_min_vol_spike',
    'web_sort', 'web_compact', 'web_result_page', 'web_result_stock',
]

RESEARCH_DEFAULTS = {
    'web_preset': '不限條件', 'web_industry': [], 'web_max_price': 0.0,
    'web_latest': True, 'web_enable_revenue_growth': False, 'web_min_revenue_growth': 0.0,
    'web_enable_eps_growth': False, 'web_min_eps_growth': 0.0,
    'web_enable_yield_rate': False, 'web_min_yield_rate': 0.0,
    'web_enable_position_1y': False, 'web_min_position_1y': 0.0,
    'web_max_position_1y': 1.0, 'web_enable_vol_spike': False, 'web_min_vol_spike': 1.0,
    'web_sort': '代號', 'web_compact': False, 'web_result_page': 1,
    'web_result_stock': None,
}


def supabase_settings():
    """Read only public Supabase client settings from Streamlit Secrets."""
    if create_client is None:
        return None, None
    try:
        section = st.secrets.get('supabase', {})
        url = section.get('url') or st.secrets.get('SUPABASE_URL')
        key = section.get('anon_key') or st.secrets.get('SUPABASE_ANON_KEY')
    except Exception:
        return None, None
    return url, key


def get_supabase_client():
    if 'web_supabase_client' in st.session_state:
        return st.session_state.web_supabase_client
    url, key = supabase_settings()
    if not url or not key:
        return None
    try:
        if ClientOptions is not None:
            client = create_client(url, key, options=ClientOptions(flow_type='pkce'))
        else:
            client = create_client(url, key)
    except Exception:
        return None
    st.session_state.web_supabase_client = client
    return client


def response_data(response):
    return getattr(response, 'data', None) or []


def authenticated_user(response):
    """Get the user object returned by Supabase auth responses."""
    user = getattr(response, 'user', None)
    if user is None:
        data = getattr(response, 'data', None)
        user = getattr(data, 'user', None)
    return user


def authenticated_session(response):
    """Get a session returned by sign-up/sign-in responses, if any."""
    session = getattr(response, 'session', None)
    if session is None:
        data = getattr(response, 'data', None)
        session = getattr(data, 'session', None)
    return session


def signed_in_user(client):
    return st.session_state.get('web_supabase_user')


def auth_redirect_url():
    """The URL Supabase uses after email confirmation/recovery."""
    try:
        return st.secrets.get('SUPABASE_REDIRECT_URL') or (
            st.secrets.get('supabase', {}).get('redirect_url')
        ) or 'https://family-stock-app-8rw6hwkeyxshkeelyygjle.streamlit.app/'
    except Exception:
        return 'https://family-stock-app-8rw6hwkeyxshkeelyygjle.streamlit.app/'


def recover_password_session(client):
    """Exchange a Supabase PKCE recovery code returned in the App URL."""
    if st.session_state.get('web_password_recovery'):
        return True
    code = str(st.query_params.get('code', '') or '').strip()
    access_token = str(st.query_params.get('access_token', '') or '').strip()
    refresh_token = str(st.query_params.get('refresh_token', '') or '').strip()
    if not code and not (access_token and refresh_token):
        return False
    try:
        if code:
            response = client.auth.exchange_code_for_session(code)
        else:
            response = client.auth.set_session(access_token, refresh_token)
        user = authenticated_user(response)
        if user is None:
            current = client.auth.get_user()
            user = getattr(current, 'user', None)
        if user is None:
            raise ValueError('重設連結已開啟，但沒有取得有效的重設工作階段')
        st.session_state.web_supabase_user = user
        st.session_state.web_password_recovery = True
        st.query_params.clear()
        return True
    except Exception as exc:
        st.session_state.web_password_recovery_error = str(exc)
        return False


def render_password_recovery(client):
    """Render the new-password form after Supabase recovery redirect."""
    if not st.session_state.get('web_password_recovery'):
        return False
    st.info('請設定新的登入密碼。')
    with st.form('supabase_password_recovery'):
        new_password = st.text_input('新密碼', type='password')
        confirm_password = st.text_input('確認新密碼', type='password')
        submit = st.form_submit_button('更新密碼')
    if submit:
        if len(new_password) < 6:
            st.warning('密碼至少需要 6 個字元。')
        elif new_password != confirm_password:
            st.warning('兩次輸入的密碼不一致。')
        else:
            try:
                client.auth.update_user({'password': new_password})
                st.session_state.pop('web_password_recovery', None)
                st.success('密碼已更新，請使用新密碼登入。')
                client.auth.sign_out()
                st.session_state.pop('web_supabase_user', None)
            except Exception as exc:
                st.error(f'密碼更新失敗：{exc}')
    return True


def bootstrap_shared_watchlist(client, user):
    """Create the first family/member/shared list for the signed-in owner."""
    user_id = str(user.id)
    families = response_data(
        client.table('family_groups').select('*').eq('created_by', user_id).limit(1).execute()
    )
    if families:
        family = families[0]
    else:
        family = response_data(
            client.table('family_groups').insert({
                'name': '家庭共用清單', 'created_by': user_id,
            }).execute()
        )[0]
        client.table('family_members').insert({
            'family_id': family['id'], 'user_id': user_id, 'role': 'owner',
        }).execute()
    family_id = family['id']
    lists = response_data(
        client.table('watchlists').select('*').eq('family_id', family_id).eq('scope', 'shared').limit(1).execute()
    )
    if lists:
        watchlist = lists[0]
    else:
        watchlist = response_data(
            client.table('watchlists').insert({
                'family_id': family_id, 'name': '家庭共用觀察', 'scope': 'shared',
            }).execute()
        )[0]
    return family_id, watchlist['id']


def render_watchlist_sidebar(df):
    """Render optional auth and synchronized watchlist controls."""
    client = get_supabase_client()
    with st.sidebar.expander('☁️ 家庭觀察清單', expanded=False):
        if client is None:
            st.caption('尚未設定雲端連線；目前仍可使用本機研究功能。')
            return
        recover_password_session(client)
        if st.session_state.pop('web_password_recovery_error', None):
            st.error('重設連結無效或已過期，請重新寄送重設信。')
        if render_password_recovery(client):
            return
        user = signed_in_user(client)
        if user is None:
            st.caption('使用 Supabase Email＋密碼登入。')
            with st.form('supabase_password_auth'):
                email = st.text_input('家庭成員 Email', key='web_auth_email')
                password = st.text_input('密碼', type='password', key='web_auth_password')
                login = st.form_submit_button('登入')
                signup = st.form_submit_button('建立帳號')
            if (login or signup) and email.strip() and password:
                try:
                    if signup:
                        result = client.auth.sign_up({
                            'email': email.strip(),
                            'password': password,
                            'options': {'email_redirect_to': auth_redirect_url()},
                        })
                        authenticated = authenticated_user(result)
                        if authenticated is not None and authenticated_session(result) is not None:
                            st.session_state.web_supabase_user = authenticated
                            st.success('帳號已建立並登入。')
                            st.rerun()
                        else:
                            st.info('帳號已建立。請開啟確認信完成 Email 驗證，再回來按「登入」。')
                    else:
                        result = client.auth.sign_in_with_password({
                            'email': email.strip(), 'password': password,
                        })
                        authenticated = authenticated_user(result)
                        if authenticated is None:
                            raise ValueError('登入成功但沒有取得使用者資料')
                        st.session_state.web_supabase_user = authenticated
                        st.rerun()
                except Exception as exc:
                    st.error(f'操作失敗：{exc}')
            elif login or signup:
                st.warning('請輸入 Email 與密碼。')
            with st.form('supabase_signup_resend'):
                resend_email = st.text_input('沒收到確認信？輸入 Email 重新寄送', key='web_resend_email')
                resend = st.form_submit_button('重新寄送確認信')
            if resend:
                if not resend_email.strip():
                    st.warning('請輸入 Email。')
                else:
                    try:
                        client.auth.resend({
                            'type': 'signup',
                            'email': resend_email.strip(),
                            'options': {'email_redirect_to': auth_redirect_url()},
                        })
                        st.success('確認信已重新寄出，請檢查收件匣與垃圾郵件。')
                    except Exception as exc:
                        st.error(f'重新寄送失敗：{exc}')
            with st.form('supabase_password_reset_request'):
                reset_email = st.text_input('忘記密碼？輸入 Email 寄送重設連結', key='web_reset_email')
                request_reset = st.form_submit_button('寄送重設密碼信')
            if request_reset:
                if not reset_email.strip():
                    st.warning('請輸入 Email。')
                else:
                    try:
                        client.auth.reset_password_for_email(
                            reset_email.strip(),
                            options={'redirect_to': auth_redirect_url()},
                        )
                        st.success('重設連結已寄出，請開啟信件後回到 App 設定新密碼。')
                    except Exception as exc:
                        st.error(f'寄送失敗：{exc}')
            return
        try:
            family_id, watchlist_id = bootstrap_shared_watchlist(client, user)
            st.session_state.web_watchlist_context = {
                'family_id': family_id, 'watchlist_id': watchlist_id,
            }
            st.caption(f'已登入：{getattr(user, "email", "家庭成員")}')
            if st.button('登出', key='web_supabase_signout'):
                client.auth.sign_out()
                for key in ('web_supabase_user', 'web_watchlist_context'):
                    st.session_state.pop(key, None)
                st.rerun()
            items = response_data(
                client.table('watchlist_items').select('*').eq('watchlist_id', watchlist_id).order('symbol').execute()
            )
            if not items:
                st.caption('共用觀察清單目前是空的。')
            else:
                st.write('已觀察：' + '、'.join(item['symbol'] for item in items))
        except Exception as exc:
            st.warning(f'觀察清單尚未初始化：{exc}')


def watchlist_items(client):
    context = st.session_state.get('web_watchlist_context')
    if not client or not context:
        return []
    return response_data(
        client.table('watchlist_items').select('*').eq('watchlist_id', context['watchlist_id']).order('symbol').execute()
    )


def render_watchlist_action(stock_id):
    client = get_supabase_client()
    user = signed_in_user(client) if client else None
    context = st.session_state.get('web_watchlist_context')
    if not client or not user or not context:
        return
    existing = next((item for item in watchlist_items(client) if item['symbol'] == str(stock_id)), None)
    if existing:
        st.caption('⭐ 已在家庭共用觀察清單')
        note = st.text_input('觀察備註', value=existing.get('note') or '', key=f'watch_note_{stock_id}')
        c1, c2 = st.columns(2)
        if c1.button('儲存備註', key=f'watch_save_{stock_id}'):
            client.table('watchlist_items').update({
                'note': note,
                'updated_at': datetime.now(timezone.utc).isoformat(),
            }).eq('id', existing['id']).execute()
            st.success('備註已儲存')
        if c2.button('移除觀察', key=f'watch_remove_{stock_id}'):
            client.table('watchlist_items').delete().eq('id', existing['id']).execute()
            st.rerun()
    elif st.button('加入家庭共用觀察', key=f'watch_add_{stock_id}'):
        client.table('watchlist_items').insert({
            'watchlist_id': context['watchlist_id'],
            'symbol': str(stock_id),
            'added_by': str(user.id),
        }).execute()
        st.success('已加入家庭共用觀察清單')


def number(value, suffix='', signed=False):
    if value is None or pd.isna(value):
        return '未取得'
    return f'{value:+.2f}{suffix}' if signed else f'{value:,.2f}{suffix}'


def filter_stocks(df, settings):
    result = df.copy()
    if settings.get('industry'):
        result = result[result['industry'].isin(settings['industry'])]
    for column, minimum in PRESETS[settings.get('preset', '不限條件')].items():
        result = result[pd.to_numeric(result[column], errors='coerce') >= minimum]
    for column in ('revenue_growth', 'eps_growth', 'yield_rate', 'position_1y', 'vol_spike'):
        minimum = settings.get(f'{column}_min')
        maximum = settings.get(f'{column}_max')
        if minimum is not None:
            result = result[pd.to_numeric(result[column], errors='coerce') >= minimum]
        if maximum is not None:
            result = result[pd.to_numeric(result[column], errors='coerce') <= maximum]
    if settings.get('max_price', 0) > 0:
        result = result[pd.to_numeric(result['close'], errors='coerce') <= settings['max_price']]
    if settings.get('latest_only'):
        result = result[result['date'] == df['date'].max()]
    return result


def open_stock(stock_id):
    if stock_id is None:
        return
    capture_research_state()
    st.session_state.web_stock = str(stock_id)
    recent = st.session_state.get('web_recent', [])
    st.session_state.web_recent = [str(stock_id)] + [x for x in recent if x != str(stock_id)][:7]
    st.session_state.web_page = '個股詳情'
    try:
        st.query_params['stock'] = str(stock_id)
    except Exception:
        pass


def return_to_explore():
    restore_research_state()
    st.session_state.web_page = '探索選股'
    try:
        st.query_params.clear()
    except Exception:
        pass


def capture_research_state():
    st.session_state.web_research_state = {
        key: st.session_state[key]
        for key in RESEARCH_KEYS
        if key in st.session_state
    }


def restore_research_state():
    saved = st.session_state.get('web_research_state', {})
    for key in RESEARCH_KEYS:
        if key in saved:
            st.session_state[key] = saved[key]


def clear_research_state():
    st.session_state.web_reset_requested = True
    st.session_state.pop('web_applied', None)


def selection_reasons(row, settings):
    reasons = []
    for column, minimum in PRESETS[settings.get('preset', '不限條件')].items():
        value = pd.to_numeric(row.get(column), errors='coerce')
        if pd.notna(value):
            reasons.append(f'{LABELS[column]} {value:.2f} ≥ {minimum:g}')
    for column in ('revenue_growth', 'eps_growth', 'yield_rate', 'position_1y', 'vol_spike'):
        value = pd.to_numeric(row.get(column), errors='coerce')
        minimum = settings.get(f'{column}_min')
        maximum = settings.get(f'{column}_max')
        if minimum is not None:
            reasons.append(f'{LABELS[column]} {value:.2f} ≥ {minimum:g}' if pd.notna(value) else f'{LABELS[column]} 未取得（不符合）')
        if maximum is not None:
            reasons.append(f'{LABELS[column]} {value:.2f} ≤ {maximum:g}' if pd.notna(value) else f'{LABELS[column]} 未取得（不符合）')
    if settings.get('industry'):
        reasons.append('產業：' + str(row.get('industry', '未取得')))
    if settings.get('latest_only'):
        reasons.append(f"行情日：{row.get('date', '未取得')}")
    return reasons or ['未套用額外條件']


def render_detail(df, history, chart):
    stock_id = st.session_state.get('web_stock')
    selected = df[df['stock_id'] == stock_id]
    if selected.empty:
        st.info('請先從上方搜尋股票，或在探索選股點選「查看個股」。')
        return
    row = selected.iloc[0]
    st.button('← 返回探索選股', on_click=return_to_explore, key='web_back_explore')
    st.title(f"{stock_id} · {row['name']}")
    st.caption(f"{row['industry']} · 行情日期 {row['date']} · 日線資料，非即時報價")
    render_watchlist_action(stock_id)
    if row['date'] != df['date'].max():
        st.warning('此股票行情早於資料庫最新交易日，可能涉及停牌或資料缺漏，請先確認。')
    a, b = st.columns(2)
    a.metric('收盤價', number(row['close']))
    change = row['change_pct']
    sign = '' if pd.isna(change) or change == 0 else (' 🔴' if change > 0 else ' 🟢')
    b.metric('漲跌幅', number(change, '%', signed=True) + sign)
    span = st.radio('走勢範圍', ['近 3 月', '近半年', '近 1 年', '近 3 年'], horizontal=True, key='web_span')
    with st.spinner('讀取個股走勢…'):
        prices = history(stock_id)
    if prices.empty:
        st.info('尚未取得歷史行情。')
    else:
        days = {'近 3 月': 90, '近半年': 183, '近 1 年': 366, '近 3 年': 1096}[span]
        dates = pd.to_datetime(prices['date'])
        prices = prices[dates >= dates.max() - pd.Timedelta(days=days)].copy()
        figure = chart(prices, stock_id, row['name'])
        # Respect the chosen range instead of the legacy chart's 120-bar default.
        figure.update_xaxes(range=[pd.to_datetime(prices['date']).min(), pd.to_datetime(prices['date']).max() + pd.Timedelta(days=3)])
        figure.update_layout(height=440, margin=dict(l=10, r=10, t=40, b=10))
        figure.update_yaxes(autorange=True, row=2, col=1)
        st.plotly_chart(figure, width='stretch')
    st.subheader('基本面摘要')
    groups = st.tabs(['成長與獲利', '估值與股息', '量價位置'])
    fields = [
        [('revenue_growth', '營收成長率', '%'), ('eps_growth', 'EPS 成長率', '%'), ('gross_margin', '毛利率', '%'), ('eps', 'EPS', ' 元')],
        [('pe_ratio', '本益比', ' 倍'), ('pb_ratio', '股價淨值比', ' 倍'), ('yield_rate', '殖利率', '%')],
        [('vol_spike', '量能倍數', ' 倍'), ('position_1y', '近一年位階（0–1）', ''), ('volume', '成交量（股）', '')],
    ]
    for tab, group in zip(groups, fields):
        with tab:
            for field, label, unit in group:
                st.write(f'**{label}**　{number(row.get(field), unit)}')
    st.caption('基本面欄位不一定與行情同日更新；上方日期僅代表行情。未取得的值不視為 0。')
    st.subheader('閱讀提醒')
    notes = []
    if pd.notna(row.get('eps')) and row['eps'] <= 0:
        notes.append('EPS 非正值，不能只靠低本益比判斷便宜。')
    if pd.notna(row.get('vol_spike')) and row['vol_spike'] >= 1.5:
        notes.append('量能達均量 1.5 倍以上；放量本身不代表後續一定上漲。')
    if pd.notna(row.get('position_1y')) and row['position_1y'] >= .8:
        notes.append('股價接近近一年區間高位，留意波動與追價風險。')
    for note in notes:
        st.write('• ' + note)
    st.caption('以上是資料條件提示，不是買賣建議；沒有觸發提示也不代表沒有風險。')


def render_explore(df):
    st.title('探索選股')
    st.caption('選擇範本與基本條件，按「套用篩選」後更新結果。完整條件仍可在進階工具使用。')
    if st.session_state.pop('web_reset_requested', False):
        for key, default in RESEARCH_DEFAULTS.items():
            st.session_state[key] = default.copy() if isinstance(default, list) else default
        st.session_state.web_research_state = {}
    else:
        restore_research_state()
    with st.form('web_filters'):
        preset = st.selectbox('篩選範本', list(PRESETS), key='web_preset')
        industries = st.multiselect('產業（不選即不限）', sorted(df['industry'].dropna().unique()), key='web_industry')
        with st.expander('更多條件'):
            max_price = st.number_input('最高股價（0 代表不限）', min_value=0.0, value=0.0, key='web_max_price')
            st.caption('下列條件需勾選才會生效；數值 0 是有效門檻，不代表不限。')
            custom = {}
            numeric_specs = [
                ('revenue_growth', '營收成長率下限', -100.0, 300.0, 0.0),
                ('eps_growth', 'EPS 成長率下限', -300.0, 300.0, 0.0),
                ('yield_rate', '殖利率下限', -100.0, 100.0, 0.0),
                ('vol_spike', '量能倍數下限', 0.0, 100.0, 1.0),
            ]
            for column, label, low, high, default in numeric_specs:
                enabled = st.checkbox(f'啟用：{label}', key=f'web_enable_{column}')
                value = st.number_input(label, min_value=low, max_value=high, value=default, step=0.1, key=f'web_min_{column}')
                custom[f'{column}_min'] = value if enabled else None
            enabled = st.checkbox('啟用：近一年位階範圍', key='web_enable_position_1y')
            position_min = st.number_input('近一年位階下限', min_value=0.0, max_value=1.0, value=0.0, step=0.05, key='web_min_position_1y')
            position_max = st.number_input('近一年位階上限', min_value=0.0, max_value=1.0, value=1.0, step=0.05, key='web_max_position_1y')
            custom['position_1y_min'] = position_min if enabled else None
            custom['position_1y_max'] = position_max if enabled else None
            latest_only = st.checkbox('僅顯示資料庫最新行情日的股票', value=True, key='web_latest')
        apply = st.form_submit_button('套用篩選', type='primary')
        reset = st.form_submit_button('清除篩選')
    if reset:
        clear_research_state()
        st.rerun()
    if apply or 'web_applied' not in st.session_state:
        st.session_state.web_applied = dict(preset=preset, industry=industries, max_price=max_price, latest_only=latest_only, **custom)
    settings = st.session_state.web_applied
    conditions = [f'{LABELS[k]} ≥ {v}' for k, v in PRESETS[settings['preset']].items()]
    if settings['industry']:
        conditions.append('產業：' + '、'.join(settings['industry']))
    if settings['max_price']:
        conditions.append(f"股價 ≤ {settings['max_price']:g}")
    for column in ('revenue_growth', 'eps_growth', 'yield_rate', 'position_1y', 'vol_spike'):
        if settings.get(f'{column}_min') is not None:
            conditions.append(f"{LABELS[column]} ≥ {settings[f'{column}_min']:g}")
        if settings.get(f'{column}_max') is not None:
            conditions.append(f"{LABELS[column]} ≤ {settings[f'{column}_max']:g}")
    if settings['latest_only']:
        conditions.append(f"行情日 {df['date'].max()}")
    st.caption('已套用：' + ('；'.join(conditions) or '不限條件'))
    result = filter_stocks(df, settings)
    st.subheader(f'符合 {len(result):,} 檔')
    if result.empty:
        st.info('沒有符合的股票。可取消產業限制、提高價格上限，或切換為「不限條件」。')
        return
    sort = st.selectbox('排序方式', ['代號', '營收成長率', '殖利率', '量能倍數'], key='web_sort')
    sort_col = {'代號': 'stock_id', '營收成長率': 'revenue_growth', '殖利率': 'yield_rate', '量能倍數': 'vol_spike'}[sort]
    result = result.sort_values(sort_col, ascending=sort == '代號', na_position='last')
    compact = st.toggle('精簡卡片（適合手機）', value=False, key='web_compact')
    ids = result['stock_id'].tolist()
    names = result.set_index('stock_id')['name'].to_dict()
    selected = st.selectbox('查看篩選結果中的股票', ids, format_func=lambda x: f'{x} {names[x]}', index=None, key='web_result_stock')
    st.button('查看個股', disabled=selected is None, on_click=open_stock, args=(selected,), key='web_result_open')
    if selected is not None:
        selected_row = result[result['stock_id'] == selected].iloc[0]
        with st.expander(f'為什麼 {selected} 符合目前條件', expanded=True):
            for reason in selection_reasons(selected_row, settings):
                st.write('• ' + reason)
    if compact:
        page = st.selectbox('結果頁', range(1, (len(result) + 11) // 12 + 1), key='web_result_page')
        for _, row in result.iloc[(page-1)*12:page*12].iterrows():
            with st.container(border=True):
                st.write(f"**{row['stock_id']} {row['name']}** · {row['industry']}")
                st.write(f"收盤 {number(row['close'])} ｜ 漲跌 {number(row['change_pct'], '%', True)}")
                st.caption(f"行情日 {row['date']}")
                with st.expander('符合條件'):
                    for reason in selection_reasons(row, settings):
                        st.write('• ' + reason)
                st.button('查看個股', key=f"card_{row['stock_id']}", on_click=open_stock, args=(row['stock_id'],))
    else:
        columns = ['stock_id', 'name', 'close', 'change_pct', 'date', 'revenue_growth', 'yield_rate', 'vol_spike']
        st.dataframe(result[columns], hide_index=True, width='stretch', column_config={
            'stock_id': '代號', 'name': '名稱', 'date': '行情日期',
            'close': st.column_config.NumberColumn('收盤價', format='%.2f'),
            'change_pct': st.column_config.NumberColumn('漲跌 %', format='%+.2f'),
            'revenue_growth': st.column_config.NumberColumn('營收成長 %', format='%+.2f'),
            'yield_rate': st.column_config.NumberColumn('殖利率 %', format='%.2f'),
            'vol_spike': st.column_config.NumberColumn('量能倍數', format='%.2f'),
        })
        st.caption('空白代表未取得；篩選條件只是研究入口，不是獲利保證。')


def render_app(load_data, load_history, chart, legacy, db_path):
    st.markdown('''<style>
    div[data-testid="stMetricValue"] {color: inherit; font-size: 26px !important;}
    @media (max-width: 768px) {
      .block-container {padding: 1rem;}
      [data-testid="stSidebar"] {min-width: 0 !important; max-width: 90vw !important;}
    }
    </style>''', unsafe_allow_html=True)
    if st.sidebar.toggle('進階工具（原版完整介面）', key='web_legacy'):
        st.sidebar.caption('保留完整篩選、相似股票與 AI 顧問；切換開關可回新版。')
        legacy()
        return

    # File version is part of the cache key; no stale cache after snapshot replacement.
    path = Path(db_path)
    stat = path.stat()
    wal = Path(str(path) + '-wal')
    wal_version = (wal.stat().st_mtime_ns, wal.stat().st_size) if wal.exists() else (0, 0)
    version = (str(path), stat.st_mtime_ns, stat.st_size, wal_version)

    @st.cache_data(ttl=60, show_spinner=False)
    def snapshot(version):
        return load_data({'period': '1y'})

    @st.cache_data(ttl=60, show_spinner=False)
    def history(stock_id, version):
        return load_history(stock_id)

    with st.spinner('載入行情快照…'):
        df = snapshot(version)
    if df.empty:
        st.warning('尚無可讀的股票資料。請確認資料更新狀態；此頁不會自行啟動更新。')
        return
    df = df.copy()
    df['stock_id'] = df['stock_id'].astype(str)
    df = df.sort_values('stock_id')
    render_watchlist_sidebar(df)
    if not st.session_state.get('web_query_initialized'):
        query_stock = str(st.query_params.get('stock', '')).strip()
        if query_stock and query_stock in set(df['stock_id']):
            st.session_state.web_stock = query_stock
            st.session_state.web_page = '個股詳情'
        elif query_stock:
            st.warning(f'找不到股票代號「{query_stock}」，已回到首頁。')
            try:
                st.query_params.clear()
            except Exception:
                pass
        st.session_state.web_query_initialized = True
    latest = df['date'].max()
    current = int((df['date'] == latest).sum())
    st.caption(f'日線行情 · 最新資料日 {latest} · {current:,} / {len(df):,} 檔同日 · 非即時報價')
    names = df.set_index('stock_id')['name'].to_dict()
    target = st.selectbox('搜尋股票：輸入代號或名稱', df['stock_id'].tolist(), index=None,
                          format_func=lambda x: f'{x} {names[x]}', key='web_search', placeholder='例如 2330 或 台積電')
    st.button('開啟個股詳情', type='primary', disabled=target is None, on_click=open_stock, args=(target,))
    page = st.radio('主要功能', ['首頁', '探索選股', '個股詳情'], horizontal=True, key='web_page')
    if page == '個股詳情':
        render_detail(df, lambda sid: history(sid, version), chart)
    elif page == '探索選股':
        render_explore(df)
    else:
        st.title('投資研究工作台')
        st.write('搜尋股票查看走勢與基本面，或進入探索選股縮小研究範圍。')
        st.button('開始探索選股', on_click=lambda: st.session_state.update(web_page='探索選股'))
        st.subheader('最近查看')
        recent = [sid for sid in st.session_state.get('web_recent', []) if sid in names]
        if not recent:
            st.info('還沒有查看紀錄。從上方搜尋一檔股票開始。')
        for sid in recent:
            st.button(f'{sid} {names[sid]}', key=f'recent_{sid}', on_click=open_stock, args=(sid,))
        st.caption('最近查看僅保留於本次連線。永久觀察清單將於下一階段提供。')
        with st.expander('資料狀態與使用說明'):
            st.write(f'{len(df)-current:,} 檔行情早於最新資料日；可能包含停牌或缺漏，不等於全部更新失敗。')
            st.write('盤中與休市時，最新完整日線通常是上一個交易日。基本面更新日期可能不同。')
            st.write('數值保留原始資料，不截斷極端成長率；資料源原本的 0 仍會保留。')
