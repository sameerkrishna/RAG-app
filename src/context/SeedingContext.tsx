import { createContext, useContext, useState } from 'react';
import type { ReactNode } from 'react';

interface SeedingContextValue {
  isSeeding: boolean;
  setIsSeeding: (value: boolean) => void;
}

const SeedingContext = createContext<SeedingContextValue>({
  isSeeding: false,
  setIsSeeding: () => {}
});

export function SeedingProvider({ children }: { children: ReactNode }) {
  const [isSeeding, setIsSeeding] = useState(false);
  return (
    <SeedingContext.Provider value={{ isSeeding, setIsSeeding }}>
      {children}
    </SeedingContext.Provider>
  );
}

export function useSeeding() {
  return useContext(SeedingContext);
}
