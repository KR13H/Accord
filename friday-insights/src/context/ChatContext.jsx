import { createContext, useCallback, useContext, useMemo, useState } from "react";

const ChatContext = createContext(null);

export function ChatProvider({ children }) {
  const [isOpen, setIsOpen] = useState(false);
  const [messages, setMessages] = useState([
    {
      id: "welcome",
      role: "assistant",
      content: "Hi, I am Accord IT Support AI. Tell me what issue you are facing.",
    },
  ]);

  const toggleChat = useCallback(() => {
    setIsOpen((prev) => !prev);
  }, []);

  const addMessage = useCallback((message) => {
    if (!message || !message.role || !message.content) {
      return;
    }
    setMessages((prev) => [
      ...prev,
      {
        id: message.id || `msg_${Date.now()}_${Math.random().toString(36).slice(2, 7)}`,
        role: message.role,
        content: message.content,
      },
    ]);
  }, []);

  const clearChat = useCallback(() => {
    setMessages([]);
  }, []);

  const value = useMemo(
    () => ({ isOpen, messages, toggleChat, addMessage, clearChat }),
    [isOpen, messages, toggleChat, addMessage, clearChat]
  );

  return <ChatContext.Provider value={value}>{children}</ChatContext.Provider>;
}

export function useChatContext() {
  const context = useContext(ChatContext);
  if (!context) {
    throw new Error("useChatContext must be used inside ChatProvider");
  }
  return context;
}
