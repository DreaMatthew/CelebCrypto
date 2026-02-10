#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Gemini Multimodal Dataset "Fill-in-the-Blank" Processing Script (v3 - Prompt & Fix)
Features:
1. Batch process .jsonl files.
2. Manage and rotate multiple API keys.
3. Support resume capability (checkpointing).
4. (Modified) Optimized Prompt, requiring concise reasoning (2-3 sentences).
5. Ensure all communication and output are in English.
6. Use tqdm to display processing progress.
"""

import os
import json
import time
import google.generativeai as genai
from pathlib import Path
from collections import deque
from google.api_core import exceptions
from tqdm import tqdm  # <-- 1. New import

# -----------------------------------------------------------------
# 1. Key Manager Class
# -----------------------------------------------------------------
class GeminiKeyManager:
    """Class to manage and rotate Gemini API keys."""
    def __init__(self, keys):
        if not keys:
            raise ValueError("API key list cannot be empty")
        self.keys = deque(keys)
        self.total_keys = len(keys)

    def get_next_key(self):
        """Get the key at the head of the queue."""
        return self.keys[0]

    def rotate_key(self):
        """Move the current key (head) to the tail and log the event."""
        failed_key = self.keys.popleft()
        self.keys.append(failed_key)
        # (New) Modified print to work cleanly with TQDM
        tqdm.write(f"--- Key ...{failed_key[-4:]} failed or quota exhausted. Rotating...")
        tqdm.write(f"--- Next key to try: ...{self.keys[0][-4:]}")

# -----------------------------------------------------------------
# 2. API Call Function
# -----------------------------------------------------------------
def generate_gemini_response(prompt_text, image_part, key_manager, generation_config, safety_settings):
    """
    Attempt to call Gemini API using keys from the manager.
    If it fails, automatically rotate keys and retry.
    """
    
    # Max retries = total number of keys
    max_retries = key_manager.total_keys
    
    for attempt in range(max_retries):
        key = key_manager.get_next_key()
        
        try:
            # 1. Configure the current API Key
            genai.configure(api_key=key)

            # 2. Initialize Model
            model = genai.GenerativeModel('gemini-2.5-pro')

            # 3. Prepare Multimodal Input (Format: [Image, Text])
            contents = [image_part, prompt_text]

            # (Commented out to keep progress bar clean)
            # print(f"    (Using key ...{key[-4:]} [Attempt {attempt + 1}/{max_retries}])")
            
            # 4. Send API Request
            response = model.generate_content(
                contents,
                generation_config=generation_config,
                safety_settings=safety_settings
            )
            
            # 5. Success! Return result
            # print("    ...Call successful!") # (Commented out)
            return response.text

        except (exceptions.ResourceExhausted, exceptions.PermissionDenied) as e:
            # Catch "Quota Exhausted" or "Permission Denied"
            # (New) Use TQDM write method
            tqdm.write(f"    Error: Key ...{key[-4:]} quota exhausted or invalid.")
            key_manager.rotate_key() # Rotate to next key
            time.sleep(1) # Pause to avoid rate limiting
        
        except exceptions.InternalServerError as e:
            # Catch Server Errors (500-level), usually temporary
            tqdm.write(f"    Error: Internal Server Error (500). {e}")
            tqdm.write("    Retrying with next key...")
            key_manager.rotate_key()
            time.sleep(3) # Wait longer for server errors
            
        except Exception as e:
            # Catch other unexpected errors (Network, Safety settings, etc.)
            tqdm.write(f"    Unexpected error occurred: {e}")
            tqdm.write("    Retrying with next key...")
            key_manager.rotate_key()
            time.sleep(1)

    # If loop finishes, all keys failed
    tqdm.write(f"    !!! Fatal Error: Tried all {max_retries} keys and all failed.")
    return None

# -----------------------------------------------------------------
# 3. Core Orchestrator Function
# -----------------------------------------------------------------
def process_jsonl_file(input_file, output_file, key_manager, generation_config, safety_settings, empty_tags_block):
    """
    Read JSONL file, process line by line, and write to new JSONL file.
    Supports resume capability and TQDM progress bar.
    """

    # --- (New) 2. Get total lines for TQDM ---
    total_lines = 0
    try:
        with open(input_file, 'r', encoding='utf-8') as f_count:
            total_lines = sum(1 for _ in f_count)
        if total_lines == 0:
            print("--- Input file is empty. Task ended. ---")
            return
        print(f"--- Total lines in file: {total_lines} ---")
    except FileNotFoundError:
        print(f"Fatal Error: Input file not found: {input_file}")
        return
    except Exception as e:
        print(f"Warning: Could not count lines: {e}")
        # Continue even if counting fails, progress bar might just not show percentage

    # --- Resume: Check how many lines processed in output file ---
    processed_lines = 0
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                processed_lines = sum(1 for _ in f)
        except Exception as e:
            print(f"Warning: Could not read output file '{output_file}' for counting: {e}")

    if processed_lines > 0:
        print(f"--- Found {processed_lines} lines already processed. Resuming from line {processed_lines + 1}... ---")
    
    # --- Open output file in Append ('a') mode ---
    try:
        with open(input_file, 'r', encoding='utf-8') as f_in, \
             open(output_file, 'a', encoding='utf-8') as f_out:
            
            # --- (New) 3. Wrap iterator with TQDM ---
            # total=total_lines: Total expected iterations
            # initial=processed_lines: Start visual position
            # desc: Description text
            progress_bar = tqdm(
                enumerate(f_in), 
                total=total_lines, 
                initial=processed_lines, 
                desc="Processing Progress",
                unit="lines" # Unit name
            )
            
            # Loop now uses progress_bar
            for i, line in progress_bar:
                
                # --- (Old) Skip processed lines (Still necessary logic) ---
                if i < processed_lines:
                    continue
                
                # (New) Update description to show current line number
                progress_bar.set_description(f"Processing line {i + 1}")
                
                # (Original print commented out)
                # print(f"\n--- Processing line {i + 1} ---")
                
                # 1. Parse and Extract Data
                try:
                    sample = json.loads(line)
                    
                    # 1a. Extract Human Prompt (Rules + Data)
                    human_prompt_raw = sample['conversations'][0]['value']
                    # Remove <image> placeholder
                    human_prompt_cleaned = human_prompt_raw.lstrip("<image>").lstrip()
                    
                    # 1b. Extract GPT Template (Partially filled)
                    gpt_template = sample['conversations'][1]['value']

                    # 1c. Extract Image Path
                    image_path_str = sample['images'][0]
                    image_file = Path(image_path_str)
                    
                    # 1d. Load Image (Assuming PNG)
                    image_part = {
                        'mime_type': 'image/png', # If JPEG, change to 'image/jpeg'
                        'data': image_file.read_bytes()
                    }
                    # (Commented out)
                    # print(f"    Loaded image: {image_path_str}")

                except FileNotFoundError:
                    # (New) Write error to progress bar
                    progress_bar.write(f"    Error: Image file not found {image_path_str}, skipping line {i+1}.")
                    continue
                except (KeyError, IndexError, json.JSONDecodeError) as e:
                    progress_bar.write(f"    Error: JSON format error or missing key in line {i + 1}: {e}, skipping.")
                    continue
                except Exception as e:
                    progress_bar.write(f"    Error: Unknown error parsing/loading line {i + 1}: {e}, skipping.")
                    continue
                    
                # 2. (New) Construct "Fill-in-the-Blank" Prompt (Full English)
                # ⬇️ ========== [PROMPT OPTIMIZATION] ========== ⬇️
                task_instructions = f"""
---
### Your Task: Complete the Analysis
---

You will receive a **partially completed** analysis report.
This report already contains:
1.  `<sentiment_breakdown>`: Sentiment judgments for all signals.
2.  `<prediction>`: **The final price prediction for window T+1 (This is the "established fact" you must adopt)**.

Your **sole mission** is to act as an expert market analyst and **generate and complete** the following three missing XML tags:
1.  `<reason>`
2.  `<confidence>`
3.  `<keyFeaturesUsed>`

#### Reasoning Requirements (IMPORTANTLY UPDATED):
* Your `<reason>` must be **concise and direct**, written in **2-3 sentences maximum**.
* It must **logically justify** the **established `<prediction>`** by identifying the *most critical* factor(s) (e.g., the dominant Core signal, the visual momentum, or a rule-based contradiction).
* **Do NOT** provide a long, step-by-step summary of the 4-step path. Just state the final, decisive link.
* **If the signals seem to contradict the prediction** (e.g., signals are `Bullish`, but the prediction is `Bearish`), your job is to find a rational explanation (e.g., "Despite the celebrity signals, the visual momentum at the end of the K-line chart shows 'Exhaustion' with shrinking volume, so a reversal in T+1 is predicted, hence `Bearish`.")
* Your output **must and only** be these three tags and their content (starting from `<reason>` and ending with `</keyFeaturesUsed>`).
* **You must respond in English.**

---
### Data and Rules to Analyze
---
{human_prompt_cleaned}

---
### Report to Complete
---
{gpt_template}
"""
                # ⬆️ ========== [PROMPT OPTIMIZATION END] ========== ⬆️

                # 3. Call Gemini API
                generated_fill_in = generate_gemini_response(
                    task_instructions, 
                    image_part, 
                    key_manager,
                    generation_config,
                    safety_settings
                )
                
                # 4. Write back or Terminate
                if generated_fill_in:
                    # (New) Inject Gemini's three tags into the template
                    # .strip() removes potential extra whitespace from Gemini
                    final_gpt_value = gpt_template.replace(empty_tags_block, generated_fill_in.strip())
                    
                    # Critical Check: Ensure replacement succeeded
                    if final_gpt_value == gpt_template:
                         progress_bar.write(f"    !!! Warning: Failed to replace empty tags (Line {i+1})!")
                         progress_bar.write(f"    Please check if 'EMPTY_TAGS_BLOCK' perfectly matches your .jsonl file.")
                         progress_bar.write(f"    (Raw content from Gemini: {generated_fill_in})")
                         # Continue to next line, but data for this line will be incorrect
                    
                    # Update sample
                    sample['conversations'][1]['value'] = final_gpt_value
                    
                    # Write to output file
                    f_out.write(json.dumps(sample, ensure_ascii=False) + '\n')
                    f_out.flush() # Flush to disk immediately
                    # (Commented out)
                    # print(f"--- Line {i + 1} processed and saved. ---")
                else:
                    # Fatal Error: All keys failed
                    progress_bar.write("\n\n" + "="*30)
                    progress_bar.write("🚨 Task Terminated! 🚨")
                    progress_bar.write("All API keys failed.")
                    progress_bar.write("Please check your key quotas or network connection.")
                    progress_bar.write(f"Script stopped at line {i + 1} of input file.")
                    progress_bar.write("="*30)
                    progress_bar.close() # (New) Close bar before return
                    return # Terminate script

            progress_bar.close() # (New) Close bar after normal completion

        print("\n🎉 --- All lines processed --- 🎉")

    except FileNotFoundError:
        print(f"Fatal Error: Input file not found: {input_file}")
    except Exception as e:
        print(f"Unexpected fatal error: {e}")

# -----------------------------------------------------------------
# 5. Main Execution
# -----------------------------------------------------------------
if __name__ == "__main__":
    
    # ******************************************************
    # 1. Fill in your API Keys
    # ******************************************************
    API_KEYS = [
        # Example: "YOUR_API_KEY_001",
        # Example: "YOUR_API_KEY_002",
    ]
    
    # ******************************************************
    # 2. Fill in your file paths
    # ******************************************************
    # [!] Anonymized Paths
    INPUT_FILE = r"./data/training_dataset_sharegpt.jsonl"
    OUTPUT_FILE = r"./data/training_dataset_sharegpt_full.jsonl"
    
    # ******************************************************
    # 3. Fill in the "Empty Tags" from your .jsonl GPT field
    # !!! EXTREMELY IMPORTANT !!!
    # This must match your file EXACTLY, including spaces and newlines
    # ******************************************************
    
    # ⬇️ ========== [BUG FIX] ========== ⬇️
    # Fixed Indentation and Newlines 
    # to precisely match the format in the .jsonl file
    EMPTY_TAGS_BLOCK = (
        "    <reason>\n"
        "    \n"
        "    </reason>\n"
        "    <confidence>\n"
        "    \n"
        "    </confidence>\n"
        "    <keyFeaturesUsed>\n"
        "    \n"
        "    </keyFeaturesUsed>"
    )
    # ⬆️ ========== [BUG FIX END] ========== ⬆️

    # --- Basic Checks ---
    if not API_KEYS or (API_KEYS and API_KEYS[0].startswith("YOUR_API_KEY")):
        print("Error: Please fill in your REAL API keys in the 'API_KEYS' list!")
        exit()
    if INPUT_FILE == "path/to/your/input_data.jsonl":
        print("Error: Please modify 'INPUT_FILE' and 'OUTPUT_FILE' paths!")
        exit()
        
    # ******************************************************
    # 4. Define Generation Config
    # ******************************************************
    GENERATION_CONFIG = {
        "temperature": 0.2,  # Slightly higher T allows creativity in reasoning
        "top_p": 0.9,
        "top_k": 32,
        "max_output_tokens": 4096, # Ensure enough space for XML
    }

    # (Important) Disable safety settings to prevent financial terms from being blocked
    SAFETY_SETTINGS = [
        {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
    ]

    # ******************************************************
    # 5. Initialize and Run
    # ******************************************************
    print("--- Task Started (Mode: Fill-in-the-Blank Inference) ---")
    
    # 1. Initialize Key Manager
    key_manager = GeminiKeyManager(API_KEYS)
    print(f"Loaded {key_manager.total_keys} API keys.")
    print(f"Input File: {INPUT_FILE}")
    print(f"Output File: {OUTPUT_FILE}")

    # 2. Run Main Process
    process_jsonl_file(
        INPUT_FILE,
        OUTPUT_FILE,
        key_manager,
        GENERATION_CONFIG,
        SAFETY_SETTINGS,
        EMPTY_TAGS_BLOCK
    )