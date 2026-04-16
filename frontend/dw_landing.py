"""DataWeave landing / auth helpers for app_t3.py."""
import streamlit as st


def enter_app_from_starter(display: str, method: str) -> None:
    """Mark the session as authenticated and rerun into the main app."""
    st.session_state.authenticated = True
    st.session_state.dw_logged_in = True
    st.session_state.dw_user_display = display
    st.session_state.dw_auth_method = method
    st.rerun()


def render_starter_page() -> None:
    """Render the landing / sign-in page shown before authentication."""
    st.markdown(
        """
        <style>
        [data-testid="stAppViewContainer"] { background: #0b0114; }
        .dw-land-hero {
            max-width: 480px; margin: 6vh auto 2rem auto;
            background: linear-gradient(148deg, #22182e 0%, #1a1224 100%);
            border: 1px solid rgba(168,85,247,0.35);
            border-radius: 18px; padding: 2.2rem 2.4rem;
            box-shadow: 0 24px 64px rgba(0,0,0,0.55), 0 0 64px -16px rgba(168,85,247,0.2);
            color: #e2d9f3;
        }
        .dw-land-hero h1 { color: #fff; font-size: 1.7rem; margin: 0 0 0.35rem 0; font-weight: 700; }
        .dw-land-hero p  { color: #b0a8bf; font-size: 0.92rem; margin: 0 0 1.4rem 0; }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="dw-land-hero">', unsafe_allow_html=True)
    st.markdown(
        "<h1>DATA WEAVE</h1>"
        "<p>AI-powered ETL pipeline. Sign in to continue.</p>",
        unsafe_allow_html=True,
    )

    view = st.session_state.get("dw_auth_view", "register")

    if view == "login":
        st.markdown("#### Sign in")
        if st.button("Continue with Google (demo)", key="land_google_login", use_container_width=True, type="primary"):
            enter_app_from_starter("Google user", "Google")
        st.divider()
        with st.form("land_login_form"):
            email = st.text_input("Email", placeholder="you@example.com", key="land_log_email")
            st.text_input("Password", type="password", placeholder="••••••••", key="land_log_pw")
            submitted = st.form_submit_button("Sign in", use_container_width=True)
        if submitted:
            val = (email or "").strip()
            if val:
                enter_app_from_starter(val.split("@", 1)[0] if "@" in val else val, "Email")
            else:
                st.warning("Enter your email to continue.")
        if st.button("Create an account", key="land_switch_register", type="secondary", use_container_width=True):
            st.session_state.dw_auth_view = "register"
            st.rerun()

    else:
        st.markdown("#### Create an account")
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Google (demo)", key="land_google_reg", use_container_width=True, type="primary"):
                enter_app_from_starter("Google user", "Google")
        with col2:
            if st.button("Apple (demo)", key="land_apple_reg", use_container_width=True):
                enter_app_from_starter("Apple user", "Apple")
        st.divider()
        fn_col, ln_col = st.columns(2)
        with fn_col:
            st.text_input("First name", placeholder="First", key="land_fn", label_visibility="collapsed")
        with ln_col:
            st.text_input("Last name", placeholder="Last", key="land_ln", label_visibility="collapsed")
        st.text_input("Email", placeholder="Email", key="land_email", label_visibility="collapsed")
        st.text_input("Password", type="password", placeholder="Password", key="land_pw", label_visibility="collapsed")
        agree = st.checkbox("I agree to the Terms & Conditions (demo)", key="land_terms")
        if st.button("Create account", key="land_reg_submit", type="primary", use_container_width=True, disabled=not agree):
            fn = (st.session_state.get("land_fn") or "").strip()
            ln = (st.session_state.get("land_ln") or "").strip()
            email = (st.session_state.get("land_email") or "").strip()
            if not email:
                st.warning("Please enter your email.")
            else:
                display = f"{fn} {ln}".strip() or email.split("@", 1)[0]
                enter_app_from_starter(display, "Registered (demo)")
        if st.button("Already have an account? Sign in", key="land_switch_login", type="secondary", use_container_width=True):
            st.session_state.dw_auth_view = "login"
            st.rerun()

    st.markdown("</div>", unsafe_allow_html=True)
