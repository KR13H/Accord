import React, { createContext, useContext, useMemo, useState } from "react";

type SpvContextValue = {
  activeSpvId: string;
  setActiveSpvId: (spvId: string) => void;
};

const SpvContext = createContext<SpvContextValue | undefined>(undefined);

export function SpvProvider({ children }: { children: React.ReactNode }) {
  const [activeSpvId, setActiveSpvId] = useState<string>("SPV-NOIDA-1");

  const value = useMemo(
    () => ({ activeSpvId, setActiveSpvId }),
    [activeSpvId]
  );

  return <SpvContext.Provider value={value}>{children}</SpvContext.Provider>;
}

export function useSpvContext(): SpvContextValue {
  const context = useContext(SpvContext);
  if (!context) {
    throw new Error("useSpvContext must be used within SpvProvider");
  }
  return context;
}
