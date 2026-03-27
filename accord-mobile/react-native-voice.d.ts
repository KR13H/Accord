declare module "@react-native-voice/voice" {
  type SpeechEvent = { value?: string[] };
  type SpeechError = { error?: { message?: string } };

  const Voice: {
    onSpeechResults: ((event: SpeechEvent) => void) | null;
    onSpeechError: ((event: SpeechError) => void) | null;
    onSpeechEnd: (() => void) | null;
    start(locale: string): Promise<void>;
    stop(): Promise<void>;
    destroy(): Promise<void>;
    removeAllListeners(): void;
  };

  export default Voice;
}
