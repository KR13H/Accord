import * as Haptics from "expo-haptics";
import { Platform } from "react-native";

export function useAccordHaptics() {
  const triggerSuccess = async () => {
    if (Platform.OS !== "web") {
      await Haptics.notificationAsync(Haptics.NotificationFeedbackType.Success);
    }
  };

  const triggerError = async () => {
    if (Platform.OS !== "web") {
      await Haptics.notificationAsync(Haptics.NotificationFeedbackType.Error);
    }
  };

  const triggerTap = async () => {
    if (Platform.OS !== "web") {
      await Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Light);
    }
  };

  const triggerHeavySync = async () => {
    if (Platform.OS !== "web") {
      await Haptics.impactAsync(Haptics.ImpactFeedbackStyle.Heavy);
    }
  };

  return { triggerSuccess, triggerError, triggerTap, triggerHeavySync };
}
