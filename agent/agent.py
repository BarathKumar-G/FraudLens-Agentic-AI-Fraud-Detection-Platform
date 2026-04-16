import os
import json
import re
from groq import Groq
from dotenv import load_dotenv

load_dotenv()

client = Groq(api_key=os.getenv("GROQ_API_KEY"))

SYSTEM_PROMPT = """
You are a fraud detection AI agent.

You are given:
- transaction details
- ML model output (fraud_probability, risk_tier)

Your job:
1. Explain why the transaction is risky (or safe)
2. Decide action:
   - allow
   - monitor
   - flag
   - block

Rules:
- DO NOT change the ML prediction
- Use ML output as primary signal
- Be concise
- STRICTLY return JSON only

Format:
{
  "reason": "...",
  "action": "allow | monitor | flag | block"
}
"""

def run_agent(transaction: dict) -> dict:
    prompt = f"""
Transaction:
{json.dumps(transaction, indent=2)}

Respond with JSON only.
"""

    try:
        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",   
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            response_format={"type": "json_object"}
        )

        content = response.choices[0].message.content.strip()

        try:
            match = re.search(r"\{.*\}", content, re.DOTALL)
            if match:
                return json.loads(match.group())
            else:
                return json.loads(content)

        except Exception:
            return {
                "reason": content,
                "action": "monitor"
            }

    except Exception as e:
        return {
            "reason": "LLM failed: unable to generate valid response",
            "action": "monitor"
        }