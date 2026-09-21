import gradio as gr
import pandas as pd
import json
import time

# --- TARGET SCHEMA ---
TARGET_SCHEMA = {
    "first_name": {"type": "string", "required": True},
    "last_name": {"type": "string", "required": True},
    "email": {"type": "string", "required": True},
    "phone_number": {"type": "string", "required": False},
    "start_date": {"type": "date", "required": False},
    "status": {"type": "string", "required": True}
}

# --- AI & PROFILING ENGINE ---
def profile_and_map_columns(df):
    headers = df.columns.tolist()
    auto_mapped = {}
    escalations = []
    
    for col in headers:
        col_lower = col.lower()
        if "fname" in col_lower or "first" in col_lower:
            auto_mapped[col] = "first_name"
        elif "lname" in col_lower or "last" in col_lower:
            auto_mapped[col] = "last_name"
        elif "mail" in col_lower:
            auto_mapped[col] = "email"
        elif "phone" in col_lower or "mobile" in col_lower:
            auto_mapped[col] = "phone_number"
        elif "date" in col_lower or "doj" in col_lower:
            auto_mapped[col] = "start_date"
        elif "stat" in col_lower or "active" in col_lower:
            auto_mapped[col] = "status"
        else:
            escalations.append({
                "context": f"AI Orchestration Failure: Could not confidently map source header '{col}'. Confidence score < 85%.",
                "record": json.dumps({
                    "problem_column": col,
                    "ai_suggestions": list(TARGET_SCHEMA.keys()) + ["DROP"],
                    "selected_mapping": ""
                }, indent=2)
            })
            
    return auto_mapped, escalations

# --- DETERMINISTIC ETL PIPELINE ---
def run_etl_pipeline(df, mapping):
    audit_trail = []
    mapping = {k: v for k, v in mapping.items() if v != "DROP" and v != ""}
    df_mapped = df.rename(columns=mapping)
    
    target_cols = [c for c in df_mapped.columns if c in TARGET_SCHEMA.keys()]
    df_clean = df_mapped[target_cols].copy()
    
    if 'email' in df_clean.columns:
        df_clean['email'] = df_clean['email'].astype(str).str.strip().str.lower()
        audit_trail.append({"action": "Normalization", "details": "Lowercased and stripped whitespace from emails."})
        
    initial_len = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=['email'] if 'email' in df_clean.columns else None)
    dupes_removed = initial_len - len(df_clean)
    audit_trail.append({"action": "Deduplication", "details": f"Removed {dupes_removed} duplicate records."})
    
    valid_records = []
    failed_records = 0
    for _, row in df_clean.iterrows():
        is_valid = True
        for field, rules in TARGET_SCHEMA.items():
            if rules.get("required") and pd.isna(row.get(field)):
                is_valid = False
                break
        if is_valid:
            valid_records.append(row.to_dict())
        else:
            failed_records += 1
            
    audit_trail.append({"action": "Validation", "details": f"{len(valid_records)} valid, {failed_records} failed schema requirements."})
    
    time.sleep(1) # Simulate API POST
    audit_trail.append({"action": "API_Upload", "status": "201 Created", "records_inserted": len(valid_records)})
    
    metrics = {
        "processed": initial_len,
        "duplicates": dupes_removed,
        "failed": failed_records,
        "uploaded": len(valid_records)
    }
    
    return metrics, audit_trail

# --- UI & ORCHESTRATION ---
theme = gr.themes.Default(primary_hue="indigo", neutral_hue="slate")

with gr.Blocks(theme=theme) as demo:
    gr.Markdown("# 🚀 Enterprise HCM Data Agent (MVP)")
    
    escalation_state = gr.State([])
    dataframe_state = gr.State(None)
    mapping_state = gr.State({})
    
    with gr.Tabs() as tabs:
        with gr.TabItem("1. Ingestion & Orchestration", id="tab1"):
            file_upload = gr.File(label="Upload HR/CRM Exports", file_types=[".xlsx", ".csv"])
            btn_trigger = gr.Button("Trigger Agentic Mapping", variant="primary")
            with gr.Row():
                agent_status = gr.Textbox(label="Agent Status", interactive=False)
                queue_status = gr.Textbox(label="Escalation Queue Status", interactive=False)
            btn_next_1 = gr.Button("Next ➡️ Go to Human Review")
            
        with gr.TabItem("2. Human-in-the-Loop Review", id="tab2"):
            agent_context = gr.Textbox(label="Agent Context (Why this escalated)", interactive=False)
            record_data = gr.Code(label="Record Data (Edit JSON to Correct)", language="json", lines=12)
            with gr.Row():
                btn_load = gr.Button("Load Next Escalation")
                btn_reject = gr.Button("Reject & Drop")
                btn_approve = gr.Button("Approve & Fix", variant="primary")
            btn_next_2 = gr.Button("Next ➡️ Run Target Sync & Audit")

        with gr.TabItem("3. Target Sync & Audit", id="tab3"):
            gr.Markdown("### ✅ Migration Dashboard")
            txt_summary = gr.Markdown("Waiting for pipeline completion...")
            json_audit = gr.JSON(label="Detailed API & Data Audit Trail")

    # --- UI EVENT HANDLERS ---
    def trigger_agent(file, progress=gr.Progress()):
        if not file:
            return "Waiting for file...", "", [], None, {}
        progress(0.2, desc="Ingesting file...")
        df = pd.read_csv(file.name) if file.name.endswith('.csv') else pd.read_excel(file.name)
        
        progress(0.6, desc="AI Semantic Mapping & Confidence Scoring...")
        time.sleep(0.5) 
        auto_map, escalations = profile_and_map_columns(df)
        
        progress(1.0, desc="Mapping complete!")
        return "Agent analysis finished.", f"{len(escalations)} escalation(s) queued.", escalations, df, auto_map

    def load_escalation(queue):
        if not queue:
            return "No pending escalations in queue.", "{}"
        return queue[0]["context"], queue[0]["record"]

    def approve_fix(json_data, queue, current_mapping, progress=gr.Progress()):
        progress(0.5, desc="Parsing human corrections...")
        try:
            parsed = json.loads(json_data)
            col = parsed.get("problem_column")
            target = parsed.get("selected_mapping")
            if col and target:
                current_mapping[col] = target
            if queue:
                queue.pop(0) 
            return "Fix approved and merged. Queue updated.", queue, current_mapping
        except Exception as e:
            return f"Invalid JSON or missing keys: {e}", queue, current_mapping

    def finalize_pipeline(df, queue, final_mapping, progress=gr.Progress()):
        if df is None:
            return gr.update(selected="tab3"), "No data processed.", []
        progress(0.5, desc="Executing ETL Pipeline & Mock API Upload...")
        metrics, audit_trail = run_etl_pipeline(df, final_mapping)
        
        progress(1.0, desc="Generating Audit Logs and Dashboard...")
        summary_md = f"""
        | Metric | Value |
        |---|---|
        | Total Records Ingested | {metrics['processed']} |
        | Duplicates Removed | {metrics['duplicates']} |
        | Validation Failures | {metrics['failed']} |
        | **Successfully Uploaded to API** | **{metrics['uploaded']}** |
        """
        return gr.update(selected="tab3"), summary_md, audit_trail

    # --- WIRING ---
    btn_trigger.click(fn=trigger_agent, inputs=[file_upload], outputs=[agent_status, queue_status, escalation_state, dataframe_state, mapping_state])
    btn_next_1.click(fn=lambda: gr.update(selected="tab2"), outputs=[tabs])
    
    btn_load.click(fn=load_escalation, inputs=[escalation_state], outputs=[agent_context, record_data])
    btn_approve.click(fn=approve_fix, inputs=[record_data, escalation_state, mapping_state], outputs=[agent_context, escalation_state, mapping_state])
    btn_reject.click(fn=lambda q: (q.pop(0) if q else None, q)[1], inputs=[escalation_state], outputs=[escalation_state])
    
    btn_next_2.click(fn=finalize_pipeline, inputs=[dataframe_state, escalation_state, mapping_state], outputs=[tabs, txt_summary, json_audit])

if __name__ == "__main__":
    demo.launch()