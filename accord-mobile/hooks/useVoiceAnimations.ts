/**
 * Voice Recording Animations Hook
 * 
 * Provides fluid animations for voice recording UI states
 * - Pulsing microphone icon
 * - Expanding sound wave
 * - Fade-in/out for transcription text
 * - Loading spinner for processing
 */

import { useEffect } from 'react';
import Animated, {
  useSharedValue,
  useAnimatedStyle,
  withRepeat,
  withTiming,
  interpolate,
  Extrapolate,
  withSequence,
  withDelay,
} from 'react-native-reanimated';
import { AnimationDurations } from '../AccordTheme';

interface VoiceAnimationState {
  isListening: boolean;
  isProcessing: boolean;
}

/**
 * Pulsing microphone animation (scales and fades)
 */
export const usePulsingAnimation = (isActive: boolean) => {
  const scale = useSharedValue(1);
  const opacity = useSharedValue(1);

  useEffect(() => {
    if (isActive) {
      scale.value = withRepeat(
        withSequence(
          withTiming(1.2, { duration: AnimationDurations.default }),
          withTiming(1, { duration: AnimationDurations.default })
        ),
        -1,
        true
      );
      opacity.value = withRepeat(
        withSequence(
          withTiming(0.6, { duration: AnimationDurations.default }),
          withTiming(1, { duration: AnimationDurations.default })
        ),
        -1,
        true
      );
    } else {
      scale.value = 1;
      opacity.value = 1;
    }
  }, [isActive]);

  return useAnimatedStyle(() => ({
    transform: [{ scale: scale.value }],
    opacity: opacity.value,
  }));
};

/**
 * Wave animation (multiple expanding circles)
 */
export const useWaveAnimation = (isActive: boolean, waveIndex: number = 0) => {
  const scale = useSharedValue(0.8);
  const opacity = useSharedValue(1);

  useEffect(() => {
    if (isActive) {
      const delay = waveIndex * 200;
      scale.value = withDelay(
        delay,
        withRepeat(
          withSequence(
            withTiming(1.5, { duration: 1000 }),
            withTiming(0.8, { duration: 0 })
          ),
          -1,
          false
        )
      );
      opacity.value = withDelay(
        delay,
        withRepeat(
          withSequence(
            withTiming(0, { duration: 1000 }),
            withTiming(1, { duration: 0 })
          ),
          -1,
          false
        )
      );
    } else {
      scale.value = 0.8;
      opacity.value = 1;
    }
  }, [isActive, waveIndex]);

  return useAnimatedStyle(() => ({
    transform: [{ scale: scale.value }],
    opacity: opacity.value,
  }));
};

/**
 * Transcription fade-in animation
 */
export const useFadeAnimation = (isVisible: boolean) => {
  const opacity = useSharedValue(isVisible ? 1 : 0);

  useEffect(() => {
    opacity.value = withTiming(isVisible ? 1 : 0, {
      duration: AnimationDurations.default,
    });
  }, [isVisible]);

  return useAnimatedStyle(() => ({
    opacity: opacity.value,
  }));
};

/**
 * Rotating spinner animation
 */
export const useSpinAnimation = (isActive: boolean) => {
  const rotation = useSharedValue(0);

  useEffect(() => {
    if (isActive) {
      rotation.value = withRepeat(
        withTiming(360, { duration: 1000 }),
        -1,
        false
      );
    } else {
      rotation.value = 0;
    }
  }, [isActive]);

  return useAnimatedStyle(() => ({
    transform: [
      {
        rotate: interpolate(
          rotation.value,
          [0, 360],
          [0, 360],
          Extrapolate.CLAMP
        ) + 'deg',
      },
    ],
  }));
};

/**
 * Bounce animation (for bottom sheet or notification entrance)
 */
export const useBounceAnimation = (shouldBounce: boolean) => {
  const translateY = useSharedValue(0);

  useEffect(() => {
    if (shouldBounce) {
      translateY.value = withSequence(
        withTiming(-10, { duration: 150 }),
        withTiming(0, { duration: 150 })
      );
    }
  }, [shouldBounce]);

  return useAnimatedStyle(() => ({
    transform: [{ translateY: translateY.value }],
  }));
};

/**
 * Slide-in from bottom animation
 */
export const useSlideUpAnimation = (isVisible: boolean) => {
  const translateY = useSharedValue(isVisible ? 0 : 100);
  const opacity = useSharedValue(isVisible ? 1 : 0);

  useEffect(() => {
    translateY.value = withTiming(isVisible ? 0 : 100, {
      duration: AnimationDurations.default,
    });
    opacity.value = withTiming(isVisible ? 1 : 0, {
      duration: AnimationDurations.default,
    });
  }, [isVisible]);

  return useAnimatedStyle(() => ({
    transform: [{ translateY: translateY.value }],
    opacity: opacity.value,
  }));
};

/**
 * Scale animation (for card/button press feedback)
 */
export const useScaleAnimation = (isPressed: boolean) => {
  const scale = useSharedValue(1);

  useEffect(() => {
    scale.value = withTiming(isPressed ? 0.95 : 1, {
      duration: AnimationDurations.fast,
    });
  }, [isPressed]);

  return useAnimatedStyle(() => ({
    transform: [{ scale: scale.value }],
  }));
};

/**
 * Color fade animation
 */
export const useColorFade = (isActive: boolean, activeColor: string, inactiveColor: string) => {
  const colorValue = useSharedValue(isActive ? 1 : 0);

  useEffect(() => {
    colorValue.value = withTiming(isActive ? 1 : 0, {
      duration: AnimationDurations.default,
    });
  }, [isActive]);

  return useAnimatedStyle(() => {
    // Note: In React Native, animated colors are limited
    // This returns opacity which can be used with different views
    return {
      opacity: interpolate(colorValue.value, [0, 1], [0.5, 1]),
    };
  });
};

export default {
  usePulsingAnimation,
  useWaveAnimation,
  useFadeAnimation,
  useSpinAnimation,
  useBounceAnimation,
  useSlideUpAnimation,
  useScaleAnimation,
  useColorFade,
};
