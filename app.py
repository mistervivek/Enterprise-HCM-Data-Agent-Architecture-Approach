"""
Enterprise HCM Data Agent - Production Architecture & ETL Execution Engine
Author: Vivek Taral
"""

import pandas as pd
import json
import time
import re
import difflib
import gradio as gr
from datetime import datetime

# --- TARGET SCHEMA DEFINITION ---
TARGET_SCHEMA = {
    "first_name": {
        "type": "string",
        "required": True,
        "aliases": ["fname", "first_name", "first", "given_name", "forename"]
    },
    "last_name": {
        "type": "string",
        "required": True,
        "aliases": ["lname", "last_name", "last", "surname", "family_name"]
    },
    "email": {
        "type": "string",
        "required": True,
        "aliases": ["email", "e-mail", "work_email", "mail", "mail_address", "email_id"]
    },
    "phone_number": {
        "type": "string",
        "required": False,
        "aliases": ["phone", "mobile", "cell", "contact_no", "telephone", "phone_number"]
    },
    "start_date": {
        "type": "date",
        "required": False,
        "aliases": ["start_date", "doj", "date_of_joining", "hire_date", "joining_date", "date"]
    },
    "status": {
        "type": "string",
        "required": True,
        "aliases": ["status", "emp_status", "active_status", "employment_status", "stat", "active"]
    }
}

# --- MULTI-SIGNAL CONFIDENCE ENGINE ---
def compute_column_confidence(col_name):
    """
    Evaluates semantic confidence using:
    1. Exact / Alias matching
    2. Substring & Sequence similarity score (0.0 to 1.0)
    3. Absolute Confidence Threshold (> 85%)
    4. Margin of Victory Threshold (> 15%)
    """
    col_clean = col_name.lower().replace("_", " ").replace("-", " ").strip()
    candidate_scores = []
    
    for field, spec in TARGET_SCHEMA.items():
        best_field_score = 0.0
        
        # 1. Exact alias match check
        if any(alias == col_name.lower() or alias == col_clean for alias in spec["aliases"]):
            best_field_score = 1.0
        else:
            # 2. Fuzzy sequence matching & substring heuristic
            for alias in spec["aliases"]:
                alias_clean = alias.replace("_", " ")
                ratio = difflib.SequenceMatcher(None, col_clean, alias_clean).ratio()
                if alias_clean in col_clean or col_clean in alias_clean:
                    ratio = max(ratio, 0.88)
                if ratio > best_field_score:
                    best_field_score = ratio
                    
        candidate_scores.append((field, round(best_field_score, 2)))
        
    candidate_scores.sort(key=lambda x: x[1], reverse=True)
    top_candidate, top_score = candidate_scores[0]
    second_candidate, second_score = candidate_scores[1] if len(candidate_scores) > 1 else ("NONE", 0.0)
    
    margin_of_victory = round(top_score - second_score, 2)
    
    passes_abs = top_score >= 0.85
    passes_mov = margin_of_victory >= 0.15
    is_autonomous = passes_abs and passes_mov
    
    return {
        "column": col_name,
        "top_candidate": top_candidate if is_autonomous else None,
        "top_score": top_score,
        "second_candidate": second_candidate,
        "second_score": second_score,
        "margin_of_victory": margin_of_victory,
        "passes_abs": passes_abs,
        "passes_mov": passes_mov,
        "is_autonomous": is_autonomous,
        "candidate_scores": candidate_scores
    }

def profile_and_map_columns(df):
    """
    Profiles headers using Multi-Signal Confidence Engine.
    Maps high-confidence headers autonomously; escalates ambiguous ones to human review.
    """
    headers = df.columns.tolist()
    auto_mapped = {}
    escalations = []
    mapped_targets = set()
    
    for col in headers:
        eval_res = compute_column_confidence(col)
        
        if eval_res["is_autonomous"]:
            target = eval_res["top_candidate"]
            # Prevent multiple source columns from auto-mapping to the exact same target
            if target not in mapped_targets:
                auto_mapped[col] = target
                mapped_targets.add(target)
            else:
                # Target collision -> escalate to human review
                escalations.append({
                    "context": f"AI Orchestration Conflict: Column '{col}' matches target '{target}' with {eval_res['top_score']*100:.0f}% confidence, but '{target}' was already mapped to another column.",
                    "record": json.dumps({
                        "problem_column": col,
                        "ai_suggestions": [target, "DROP"] + [k for k in TARGET_SCHEMA.keys() if k != target],
                        "selected_mapping": ""
                    }, indent=2)
                })
        else:
            # Escalation reason formatting
            reason = []
            if not eval_res["passes_abs"]:
                reason.append(f"Confidence ({eval_res['top_score']*100:.0f}%) below 85% threshold")
            if not eval_res["passes_mov"]:
                reason.append(f"Margin of victory ({eval_res['margin_of_victory']*100:.0f}%) below 15% threshold (Top candidate '{eval_res['candidate_scores'][0][0]}' vs 2nd '{eval_res['second_candidate']}')")
            
            reason_str = " & ".join(reason)
            escalations.append({
                "context": f"AI Escalation Boundary Reached for '{col}': {reason_str}.",
                "record": json.dumps({
                    "problem_column": col,
                    "ai_suggestions": [eval_res['candidate_scores'][0][0], eval_res['second_candidate'], "DROP"],
                    "selected_mapping": ""
                }, indent=2)
            })
            
    return auto_mapped, escalations

# --- DETERMINISTIC ETL ENGINE ---
def run_etl_pipeline(df, mapping):
    """
    Executes cleaning, normalization, deduplication, schema validation, and mock API insertion.
    """
    audit_trail = []
    
    # 1. Clean & apply human/AI combined mapping
    mapping_clean = {k: v for k, v in mapping.items() if v and v != "DROP"}
    
    # Prevent duplicate target column collision
    rev_mapping = {}
    for src, tgt in mapping_clean.items():
        if tgt not in rev_mapping:
            rev_mapping[tgt] = src
    
    final_rename = {v: k for k, v in rev_mapping.items()}
    df_mapped = df.rename(columns=final_rename)
    
    # Keep target columns
    target_cols = [c for c in df_mapped.columns if c in TARGET_SCHEMA.keys()]
    df_clean = df_mapped[target_cols].copy()
    
    # Ensure missing target columns exist as None
    for field in TARGET_SCHEMA.keys():
        if field not in df_clean.columns:
            df_clean[field] = None

    audit_trail.append({
        "action": "Column Mapping",
        "details": f"Mapped {len(final_rename)} source columns to target schema."
    })
    
    # 2. Data Normalization & Cleaning
    if 'email' in df_clean.columns:
        def clean_email(val):
            if pd.isna(val):
                return None
            s = str(val).strip().lower()
            if s in ["", "nan", "null", "none", "n/a", "undefined"]:
                return None
            return s
        df_clean['email'] = df_clean['email'].apply(clean_email)
        audit_trail.append({
            "action": "Email Normalization",
            "details": "Lowercased, trimmed whitespace, and standardized null values for emails."
        })
        
    if 'start_date' in df_clean.columns:
        def clean_date(val):
            if pd.isna(val) or val is None:
                return None
            s = str(val).strip()
            if s in ["", "nan", "null", "none", "n/a"]:
                return None
            try:
                parsed_dt = pd.to_datetime(s)
                return parsed_dt.strftime('%Y-%m-%d')
            except Exception:
                return s # Keep raw for validation check
        df_clean['start_date'] = df_clean['start_date'].apply(clean_date)
        audit_trail.append({
            "action": "Date Normalization",
            "details": "Standardized date formats to ISO YYYY-MM-DD."
        })

    # 3. Deduplication (only on records with valid emails)
    initial_len = len(df_clean)
    valid_email_mask = df_clean['email'].notna()
    df_with_email = df_clean[valid_email_mask].copy()
    df_without_email = df_clean[~valid_email_mask].copy()
    
    initial_email_count = len(df_with_email)
    df_with_email_deduped = df_with_email.drop_duplicates(subset=['email'], keep='first')
    dupes_removed = initial_email_count - len(df_with_email_deduped)
    
    df_clean_deduped = pd.concat([df_with_email_deduped, df_without_email], ignore_index=True)
    
    audit_trail.append({
        "action": "Deduplication",
        "details": f"Detected and removed {dupes_removed} duplicate email record(s)."
    })
    
    # 4. Strict Schema Validation
    valid_records = []
    failed_records = []
    
    for idx, row in df_clean_deduped.iterrows():
        is_valid = True
        failure_reasons = []
        
        for field, spec in TARGET_SCHEMA.items():
            val = row.get(field)
            
            # Null check
            is_null = pd.isna(val) or val is None or (isinstance(val, str) and val.strip() == "")
            
            if spec.get("required") and is_null:
                is_valid = False
                failure_reasons.append(f"Required field '{field}' is missing or null")
                
            # Date validation
            if field == "start_date" and not is_null:
                if not re.match(r'^\d{4}-\d{2}-\d{2}$', str(val)):
                    is_valid = False
                    failure_reasons.append(f"Field 'start_date' value '{val}' is not in YYYY-MM-DD format")
                    
        if is_valid:
            valid_records.append(row.to_dict())
        else:
            failed_records.append({
                "row_index": idx + 1,
                "data": row.to_dict(),
                "reasons": failure_reasons
            })
            
    audit_trail.append({
        "action": "Schema Validation",
        "details": f"Validated {len(valid_records)} clean record(s); {len(failed_records)} failed schema constraints."
    })
    
    # 5. Mock API Upload
    time.sleep(0.5) # Simulating network latency
    audit_trail.append({
        "action": "API Upload Sync",
        "status": "201 Created",
        "records_inserted": len(valid_records),
        "endpoint": "POST /api/v1/hcm/workers/batch"
    })
    
    metrics = {
        "processed": initial_len,
        "duplicates": dupes_removed,
        "failed": len(failed_records),
        "uploaded": len(valid_records)
    }
    
    return metrics, audit_trail, valid_records, failed_records

# --- GRADIO WEB INTERFACE ---
def create_app():
    theme = gr.themes.Default(primary_hue="indigo", neutral_hue="slate")
    
    with gr.Blocks(title="Enterprise HCM Data Agent") as demo:
        gr.Markdown("# 🚀 Enterprise HCM Data Agent (Hybrid Architecture)")
        gr.Markdown(
            "*Autonomous Metadata Mapping with Multi-Signal Confidence Thresholds & Deterministic Auditable Python ETL Engine*"
        )
        
        # State Management
        escalation_state = gr.State([])
        dataframe_state = gr.State(None)
        mapping_state = gr.State({})
        
        with gr.Tabs() as tabs:
            # --- TAB 1: INGESTION ---
            with gr.TabItem("1. Ingestion & Orchestration", id="tab1"):
                gr.Markdown("### 📥 Step 1: Upload HR/CRM Datasets")
                file_upload = gr.File(label="Select Source Data File (.csv, .xlsx)", file_types=[".csv", ".xlsx"])
                btn_trigger = gr.Button("⚡ Trigger Agentic Mapping", variant="primary")
                
                with gr.Row():
                    agent_status = gr.Textbox(label="Agent Status", interactive=False)
                    queue_status = gr.Textbox(label="Escalation Queue Metric", interactive=False)
                    
                gr.Markdown("#### 🗺️ Initial Mapping Results")
                mapping_preview = gr.JSON(label="Auto-Mapped Columns (Autonomous Decisions >85% Confidence & >15% MoV)")
                btn_next_1 = gr.Button("Next ➡️ Proceed to Human-in-the-Loop Review")

            # --- TAB 2: HUMAN-IN-THE-LOOP REVIEW ---
            with gr.TabItem("2. Human-in-the-Loop Review", id="tab2"):
                gr.Markdown("### 🛡️ Step 2: Resolve Ambiguous Mappings (Dead Letter Escalation Queue)")
                agent_context = gr.Textbox(label="Agent Context (Mathematical Boundary Reason)", interactive=False)
                record_data = gr.Code(label="Escalation Decision Payload (Edit JSON to Correct)", language="json", lines=12)
                
                with gr.Row():
                    btn_load = gr.Button("🔄 Refresh / Load Next Escalation")
                    btn_reject = gr.Button("❌ Reject & Drop Field")
                    btn_approve = gr.Button("✅ Approve & Fix Mapping", variant="primary")
                    
                btn_next_2 = gr.Button("Next ➡️ Execute Target Sync & Audit Pipeline")

            # --- TAB 3: TARGET SYNC & DASHBOARD ---
            with gr.TabItem("3. Target Sync & Audit", id="tab3"):
                gr.Markdown("### 📊 Step 3: Migration Dashboard & Granular Audit Log")
                txt_summary = gr.Markdown("Waiting for ETL pipeline execution...")
                json_audit = gr.JSON(label="Detailed API & Data Audit Trail")

        # --- EVENT HANDLERS ---
        def trigger_agent(file, progress=gr.Progress()):
            if not file:
                return "No file uploaded.", "Queue Empty", {}, [], None, {}
            
            progress(0.2, desc="Ingesting data file...")
            try:
                if file.name.endswith('.csv'):
                    df = pd.read_csv(file.name)
                else:
                    df = pd.read_excel(file.name)
            except Exception as e:
                return f"Error reading file: {e}", "Error", {}, [], None, {}
                
            progress(0.6, desc="Running Multi-Signal Confidence Engine...")
            auto_map, escalations = profile_and_map_columns(df)
            
            progress(1.0, desc="Mapping Complete!")
            status_msg = f"Analysis complete. {len(auto_map)} columns auto-mapped, {len(escalations)} escalated."
            q_msg = f"{len(escalations)} pending escalation(s)"
            
            return status_msg, q_msg, auto_map, escalations, df, auto_map

        def get_current_escalation(queue):
            if not queue or len(queue) == 0:
                return "✅ Dead Letter Queue is empty! All column mappings resolved.", json.dumps({"status": "CLEAR", "message": "No pending escalations"}, indent=2)
            item = queue[0]
            return item["context"], item["record"]

        def approve_fix(json_data, queue, current_mapping, progress=gr.Progress()):
            progress(0.5, desc="Parsing human corrections...")
            try:
                parsed = json.loads(json_data)
                col = parsed.get("problem_column")
                target = parsed.get("selected_mapping")
                
                if col and target:
                    current_mapping[col] = target
                    
                if queue and len(queue) > 0:
                    queue.pop(0)
                    
                ctx, rec = get_current_escalation(queue)
                return ctx, rec, queue, current_mapping
            except Exception as e:
                return f"Invalid JSON format: {e}", json_data, queue, current_mapping

        def reject_fix(queue, current_mapping):
            if queue and len(queue) > 0:
                item = queue.pop(0)
                try:
                    parsed = json.loads(item["record"])
                    col = parsed.get("problem_column")
                    if col:
                        current_mapping[col] = "DROP"
                except Exception:
                    pass
            ctx, rec = get_current_escalation(queue)
            return ctx, rec, queue, current_mapping

        def finalize_pipeline(df, queue, final_mapping, progress=gr.Progress()):
            if df is None:
                return gr.update(selected="tab3"), "⚠️ No dataset loaded to process.", []
                
            progress(0.5, desc="Executing Deterministic Python ETL Pipeline...")
            metrics, audit_trail, valid_recs, failed_recs = run_etl_pipeline(df, final_mapping)
            
            progress(1.0, desc="Pipeline Finished!")
            
            summary_md = f"""
### 📈 Migration Execution Summary
| Metric | Count | Percentage |
| :--- | :---: | :---: |
| **Total Source Records** | **{metrics['processed']}** | 100% |
| **Duplicates Removed** | {metrics['duplicates']} | {(metrics['duplicates']/metrics['processed']*100 if metrics['processed'] else 0):.1f}% |
| **Validation Failures** | {metrics['failed']} | {(metrics['failed']/metrics['processed']*100 if metrics['processed'] else 0):.1f}% |
| **Successfully Inserted (API)** | **{metrics['uploaded']}** | **{(metrics['uploaded']/metrics['processed']*100 if metrics['processed'] else 0):.1f}%** |

#### 🔎 Validation Failure Breakdown
```json
{json.dumps(failed_recs, indent=2) if failed_recs else "No failed records."}
```
"""
            return gr.update(selected="tab3"), summary_md, audit_trail

        # --- WIRING ---
        btn_trigger.click(
            fn=trigger_agent,
            inputs=[file_upload],
            outputs=[agent_status, queue_status, mapping_preview, escalation_state, dataframe_state, mapping_state]
        )
        
        btn_next_1.click(
            fn=lambda q: (gr.update(selected="tab2"), get_current_escalation(q)[0], get_current_escalation(q)[1]),
            inputs=[escalation_state],
            outputs=[tabs, agent_context, record_data]
        )
        
        btn_load.click(
            fn=get_current_escalation,
            inputs=[escalation_state],
            outputs=[agent_context, record_data]
        )
        
        btn_approve.click(
            fn=approve_fix,
            inputs=[record_data, escalation_state, mapping_state],
            outputs=[agent_context, record_data, escalation_state, mapping_state]
        )
        
        btn_reject.click(
            fn=reject_fix,
            inputs=[escalation_state, mapping_state],
            outputs=[agent_context, record_data, escalation_state, mapping_state]
        )
        
        btn_next_2.click(
            fn=finalize_pipeline,
            inputs=[dataframe_state, escalation_state, mapping_state],
            outputs=[tabs, txt_summary, json_audit]
        )
        
    return demo

if __name__ == "__main__":
    app = create_app()
    app.launch(server_name="127.0.0.1", server_port=7860, share=False)
