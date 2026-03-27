import { useEffect, useMemo, useRef, useState } from "react";
import apiClient from "../../api/client";
import { useChatContext } from "../../context/ChatContext";

function buildHistory(messages) {
  return messages
    .filter((item) => item.role === "user" || item.role === "assistant")
    .map((item) => ({ role: item.role, content: item.content }));
}

export default function SupportChatWidget() {
  const { isOpen, messages, toggleChat, addMessage, clearChat } = useChatContext();
  const [input, setInput] = useState("");
  const [typing, setTyping] = useState(false);
  const [error, setError] = useState("");
  const listRef = useRef(null);

  useEffect(() => {
    if (!listRef.current) return;
    listRef.current.scrollTop = listRef.current.scrollHeight;
  }, [messages, typing]);

  const canSend = useMemo(() => input.trim().length > 0 && !typing, [input, typing]);

  const sendMessage = async () => {
    const text = input.trim();
    if (!text || typing) return;

    setInput("");
    setError("");
    addMessage({ role: "user", content: text });
    setTyping(true);

    try {
      const payload = {
        message: text,
        history: buildHistory(messages).slice(-6),
      };
      const response = await apiClient.post("/support/chat", payload);
      addMessage({ role: "assistant", content: String(response.data?.reply || "I could not generate a response.") });
    } catch (err) {
      const detail = err?.response?.data?.detail || err?.message || "Support chat failed";
      setError(detail);
      addMessage({ role: "assistant", content: "I hit a temporary error. Please try again." });
    } finally {
      setTyping(false);
    }
  };

  if (!isOpen) {
    return (
      <button
        type="button"
        onClick={toggleChat}
        className="fixed bottom-6 right-6 z-50 rounded-full border border-cyan-300/50 bg-cyan-500 px-5 py-3 text-sm font-bold text-slate-950 shadow-[0_12px_32px_rgba(6,182,212,0.45)]"
      >
        AI IT Support
      </button>
    );
  }

  return (
    <div className="fixed bottom-6 right-6 w-96 h-[500px] z-50 rounded-2xl border border-slate-700 bg-slate-950/95 shadow-2xl overflow-hidden flex flex-col">
      <div className="px-4 py-3 border-b border-slate-800 flex items-center justify-between">
        <div>
          <p className="text-xs uppercase tracking-[0.2em] text-cyan-300">Accord Support</p>
          <p className="text-sm font-semibold text-slate-100">Local AI IT Assistant</p>
        </div>
        <div className="flex items-center gap-2">
          <button type="button" onClick={clearChat} className="text-xs text-slate-400 hover:text-white">Clear</button>
          <button type="button" onClick={toggleChat} className="text-xs text-slate-400 hover:text-white">Close</button>
        </div>
      </div>

      <div ref={listRef} className="flex-1 overflow-y-auto px-3 py-3 space-y-2 bg-slate-950">
        {messages.map((msg) => (
          <div key={msg.id} className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}>
            <div
              className={`max-w-[82%] rounded-xl px-3 py-2 text-sm leading-relaxed whitespace-pre-wrap ${
                msg.role === "user"
                  ? "bg-cyan-500 text-slate-950"
                  : "bg-slate-800 text-slate-100 border border-slate-700"
              }`}
            >
              {msg.content}
            </div>
          </div>
        ))}
        {typing ? (
          <div className="flex justify-start">
            <div className="rounded-xl px-3 py-2 text-sm bg-slate-800 text-slate-300 border border-slate-700">...</div>
          </div>
        ) : null}
      </div>

      <div className="border-t border-slate-800 p-3 space-y-2 bg-slate-900/70">
        {error ? <div className="text-xs text-red-300">{error}</div> : null}
        <div className="flex items-center gap-2">
          <input
            type="text"
            value={input}
            onChange={(event) => setInput(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter") {
                event.preventDefault();
                void sendMessage();
              }
            }}
            placeholder="Describe your issue..."
            className="flex-1 rounded-lg border border-slate-700 bg-slate-950 px-3 py-2 text-sm text-slate-100 outline-none focus:border-cyan-500"
          />
          <button
            type="button"
            onClick={() => void sendMessage()}
            disabled={!canSend}
            className="rounded-lg bg-cyan-500 px-3 py-2 text-xs font-bold text-slate-950 disabled:opacity-50"
          >
            Send
          </button>
        </div>
      </div>
    </div>
  );
}
