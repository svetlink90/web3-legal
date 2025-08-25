import streamlit as st
from agents.tasks import (
    collect_address,
    sanction_screen,
    analyze_results,
    store_ack,
    anchor_log,
)

st.title("Address Sanction Screening")
address = st.text_input("Wallet address")
ack = st.checkbox("I declare that I own this address")

if st.button("Submit"):
    payload = {"address": address, "ack": ack}
    state = collect_address.run(payload)
    state = sanction_screen.run(state)
    state = analyze_results.run(state)
    state = store_ack.run(state)
    state = anchor_log.run(state)

    st.subheader("Recommendation")
    st.write(state.get("analysis", {}).get("decision"))

    st.subheader("Certificate")
    st.json(state.get("certificate", {}))
