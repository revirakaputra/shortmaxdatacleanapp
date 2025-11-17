import streamlit as st
import pandas as pd
from io import BytesIO
import time

st.title("Data Cleaner – trx_id Auto Merge & Duration Fix")

uploaded_file = st.file_uploader("Upload file XLSX", type=["xlsx"])
submit = st.button("Submit & Process")

if submit:
    if uploaded_file is None:
        st.error("Silakan upload file terlebih dahulu.")
    else:

        # PROGRESS BAR
        progress = st.progress(0)
        status_text = st.empty()

        # ==============================
        # 1. READ FILE (10%)
        # ==============================
        status_text.write("📄 Membaca file...")
        df = pd.read_excel(uploaded_file)
        progress.progress(10)
        time.sleep(0.2)

        # ==============================
        # 2. FIX duration (30%)
        # ==============================
        status_text.write("⏱ Membersihkan kolom duration...")
        df['duration'] = (
            df['duration']
            .astype(str)
            .str.replace(',', '.', regex=False)
            .astype(float)
        )
        progress.progress(30)
        time.sleep(0.2)

        # ==============================
        # 3. MERGE trx_id (60%)
        # ==============================
        status_text.write("🔗 Menggabungkan data berdasarkan trx_id...")
        result = df.groupby('trx_id').agg({

            'id': 'first',
            'msisdn': 'first',
            'package_keyword': 'first',
            'path': lambda x: ', '.join(x.unique()),

            'timestamp_request': lambda x: x.iloc[1] if len(x) > 1 else x.iloc[0],
            'timestamp': lambda x: x.iloc[0],

            'origin': lambda x: ', '.join(x.unique()),
            'destination': lambda x: ', '.join(x.unique()),

            'response_status_code': 'first',
            'status': 'first',
            'id_dashboard_portal_staging': 'first',

            'duration': 'sum'

        }).reset_index()

        progress.progress(60)
        time.sleep(0.2)

        # ==============================
        # 4. RENAME columns (80%)
        # ==============================
        status_text.write("✏️ Mengganti nama kolom...")
        result = result.rename(columns={
            'timestamp_request': 'timestamp_originate',
            'timestamp': 'terminate',
            'duration': 'duration_in_seconds'
        })

        progress.progress(80)
        time.sleep(0.2)

        # ==============================
        # 5. SELESAI (100%)
        # ==============================
        status_text.write("✅ Selesai! Data berhasil dibersihkan.")
        progress.progress(100)
        time.sleep(0.2)

        st.success("Data berhasil diproses!")

        st.dataframe(result)

        # DOWNLOAD
        output = BytesIO()
        with pd.ExcelWriter(output, engine="openpyxl") as writer:
            result.to_excel(writer, index=False)

        st.download_button(
            label="Download File Hasil (.xlsx)",
            data=output.getvalue(),
            file_name="cleaned_data.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
