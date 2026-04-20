import ReactNativeBiometrics from "react-native-biometrics";

const biometricClient = new ReactNativeBiometrics();

export async function promptAccordUnlock(): Promise<boolean> {
  try {
    const sensor = await biometricClient.isSensorAvailable();
    if (!sensor.available) {
      return false;
    }

    const result = await biometricClient.simplePrompt({
      promptMessage: "Unlock Accord Mobile",
      cancelButtonText: "Use PIN",
    });

    return Boolean(result.success);
  } catch {
    return false;
  }
}
