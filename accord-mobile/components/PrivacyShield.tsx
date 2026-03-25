import React, { useEffect, useState } from "react";
import { AppState, AppStateStatus, StyleSheet, View } from "react-native";
import { BlurView } from "expo-blur";

export function PrivacyShield({ children }: { children: React.ReactNode }) {
  const [appState, setAppState] = useState(AppState.currentState);

  useEffect(() => {
    const subscription = AppState.addEventListener("change", (nextAppState: AppStateStatus) => {
      setAppState(nextAppState);
    });

    return () => {
      subscription.remove();
    };
  }, []);

  const isBackgrounded = appState !== "active";

  return (
    <View style={styles.container}>
      {children}
      {isBackgrounded && (
        <BlurView intensity={100} tint="dark" style={StyleSheet.absoluteFill} />
      )}
    </View>
  );
}

const styles = StyleSheet.create({
  container: {
    flex: 1,
  },
});
