import json
import requests
from flask import Flask, render_template_string
from flask_socketio import SocketIO, emit
from rag import search

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

HTML = """
<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Assistant Banque Aurore</title>
  <script src="https://cdn.socket.io/4.7.5/socket.io.min.js"></script>
  <style>
    body { font-family: system-ui, sans-serif; max-width: 800px; margin: 30px auto; padding: 0 20px; }
    h1 { color: #003366; }
    #chat { border: 1px solid #ddd; border-radius: 8px; height: 500px; overflow-y: auto; padding: 15px; background: #fafafa; }
    .msg { margin: 10px 0; padding: 10px 14px; border-radius: 8px; max-width: 80%; word-wrap: break-word; }
    .user { background: #003366; color: white; margin-left: auto; }
    .bot { background: white; border: 1px solid #ddd; }
    .sources { font-size: 0.85em; color: #666; margin-top: 5px; font-style: italic; }
    .status { font-size: 0.85em; color: #999; text-align: center; margin: 5px 0; }
    #input-area { display: flex; margin-top: 15px; }
    #input { flex: 1; padding: 12px; border: 1px solid #ddd; border-radius: 8px 0 0 8px; font-size: 16px; }
    #send { padding: 12px 24px; background: #003366; color: white; border: none; border-radius: 0 8px 8px 0; cursor: pointer; }
    #send:hover { background: #0055aa; }
  </style>
</head>
<body>
  <h1>🏦 Assistant Banque Aurore</h1>
  <div id="chat"></div>
  <div id="input-area">
    <input id="input" placeholder="Posez votre question..." autofocus>
    <button id="send">Envoyer</button>
  </div>
  <script>
    const socket = io();
    const chat = document.getElementById('chat');
    const input = document.getElementById('input');
    const send = document.getElementById('send');

    function addMsg(cls, text) {
      const div = document.createElement('div');
      div.className = 'msg ' + cls;
      div.innerHTML = text;
      chat.appendChild(div);
      chat.scrollTop = chat.scrollHeight;
    }

    function sendQuestion() {
      const q = input.value.trim();
      if (!q) return;
      addMsg('user', q);
      input.value = '';
      socket.emit('ask', { question: q });
    }

    send.onclick = sendQuestion;
    input.onkeydown = (e) => { if (e.key === 'Enter') sendQuestion(); };

    // Statuts intermediaires ("Recherche...", "Generation...")
    socket.on('status', (data) => addMsg('status', '⏳ ' + data.text));

    // --- Streaming token par token ---
    let currentBot = null;

    socket.on('token', (data) => {
      if (!currentBot) {
        // Supprime les statuts des que la reponse commence
        document.querySelectorAll('.msg.status').forEach(el => el.remove());

        currentBot = document.createElement('div');
        currentBot.className = 'msg bot';
        chat.appendChild(currentBot);
      }
      currentBot.innerHTML += data.text;
      chat.scrollTop = chat.scrollHeight;
    });
    socket.on('done', (data) => {
      if (currentBot && data.sources) {
        const list = data.sources.map(s => `• ${s.source} p${s.page}`).join('<br>');
        currentBot.innerHTML += `<div class="sources">${list}</div>`;
      }
      currentBot = null;
    });

    // Cas "Je ne sais pas" (retour direct sans streaming)
    socket.on('no_answer', () => {
      addMsg('bot', '<b>Je ne sais pas.</b>');
    });
  </script>
</body>
</html>
"""

PROMPT_TEMPLATE = """Tu es un assistant bancaire de Banque Aurore.
Reponds UNIQUEMENT a partir des extraits ci-dessous.
Si la reponse n'y est pas, reponds exactement : "Je ne sais pas."
Ne fais aucune supposition. Cite toujours la source entre parentheses.

Extraits :
{contexte}

Question : {question}
Reponse :"""


@app.route("/")
def index():
    return render_template_string(HTML)


@socketio.on("ask")
def on_ask(data):
    question = data.get("question", "").strip()
    if not question:
        return

    emit("status", {"text": "Recherche dans les documents..."})
    hits = search(question, k=5)

    if not hits:
        emit("no_answer")
        return

    contexte = "\\n\\n".join(
        f"[{h['source']} p{h['page']} | {h['section']}]\\n{h['text']}"
        for h in hits
    )
    prompt = PROMPT_TEMPLATE.format(contexte=contexte, question=question)

    emit("status", {"text": "Generation de la reponse..."})

    # Streaming depuis Ollama
    try:
        with requests.post(
            "http://localhost:11434/api/generate",
            json={"model": "mistral", "prompt": prompt, "stream": True},
            stream=True,
            timeout=120,
        ) as r:
            for line in r.iter_lines():
                if not line:
                    continue
                try:
                    chunk = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if "response" in chunk:
                    emit("token", {"text": chunk["response"]})
    except Exception as e:
        emit("token", {"text": f"[Erreur Ollama: {e}]"})

    # Envoie les sources a la fin
    emit("done", {
        "sources": [{"source": h["source"], "page": h["page"]} for h in hits],
    })


if __name__ == "__main__":
    print("Serveur sur http://localhost:5000")
    socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)