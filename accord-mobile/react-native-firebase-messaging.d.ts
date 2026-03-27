declare module "@react-native-firebase/messaging" {
  export type AuthorizationStatus = number;

  export interface MessagingModule {
    requestPermission(): Promise<AuthorizationStatus>;
    registerDeviceForRemoteMessages(): Promise<void>;
    getToken(): Promise<string>;
    onTokenRefresh(listener: (token: string) => void): () => void;
  }

  export default function messaging(): MessagingModule;
}
