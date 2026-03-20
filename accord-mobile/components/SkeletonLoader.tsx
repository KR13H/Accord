/**
 * Skeleton Loader Component
 * 
 * Displays animated placeholder content while data is loading
 * Improves perceived performance and user experience
 * Reduces layout shift (CLS) on data arrival
 */

import React, { useEffect } from 'react';
import { View, ViewStyle } from 'react-native';
import Animated, {
  useSharedValue,
  useAnimatedStyle,
  withRepeat,
  withTiming,
  interpolate,
  Extrapolate,
} from 'react-native-reanimated';
import { Spacing, AccordDarkTheme } from './AccordTheme';

interface SkeletonLineProps {
  width?: string | number;
  height?: number;
  style?: ViewStyle;
  delay?: number;
}

interface SkeletonProps {
  variant?: 'line' | 'circle' | 'card' | 'avatar';
  width?: string | number;
  height?: number;
  count?: number;
  spacing?: number;
  style?: ViewStyle;
  delay?: number;
}

/**
 * Animated skeleton line component
 */
export const SkeletonLine: React.FC<SkeletonLineProps> = ({
  width = '100%',
  height = 16,
  style,
  delay = 0,
}) => {
  const progress = useSharedValue(0);

  useEffect(() => {
    progress.value = withRepeat(
      withTiming(1, { duration: 1200 }),
      -1,
      true
    );
  }, []);

  const animatedStyle = useAnimatedStyle(() => {
    const opacity = interpolate(
      progress.value,
      [0, 1],
      [0.3, 0.8],
      Extrapolate.CLAMP
    );
    return {
      opacity,
    };
  });

  return (
    <Animated.View
      style={[
        {
          width,
          height,
          backgroundColor: AccordDarkTheme.colors.surfaceVariant,
          borderRadius: 8,
          marginTop: Spacing.sm,
          marginBottom: Spacing.sm,
        },
        style,
        animatedStyle,
      ]}
    />
  );
};

/**
 * Skeleton circle (for avatars)
 */
export const SkeletonCircle: React.FC<{ size?: number; style?: ViewStyle }> = ({
  size = 40,
  style,
}) => {
  const progress = useSharedValue(0);

  useEffect(() => {
    progress.value = withRepeat(
      withTiming(1, { duration: 1200 }),
      -1,
      true
    );
  }, []);

  const animatedStyle = useAnimatedStyle(() => {
    const opacity = interpolate(
      progress.value,
      [0, 1],
      [0.3, 0.8],
      Extrapolate.CLAMP
    );
    return {
      opacity,
    };
  });

  return (
    <Animated.View
      style={[
        {
          width: size,
          height: size,
          borderRadius: size / 2,
          backgroundColor: AccordDarkTheme.colors.surfaceVariant,
        },
        style,
        animatedStyle,
      ]}
    />
  );
};

/**
 * Skeleton text block (multiple lines)
 */
export const SkeletonText: React.FC<SkeletonProps> = ({
  count = 3,
  spacing = Spacing.md,
  height = 14,
  width = '100%',
  style,
}) => {
  const lines: number[] = [];
  for (let i = 0; i < count; i++) {
    lines.push(i);
  }

  return (
    <View style={style}>
      {lines.map((i) => (
        <SkeletonLine
          key={i}
          width={i === count - 1 ? '70%' : width}
          height={height}
          style={{ marginBottom: spacing }}
        />
      ))}
    </View>
  );
};

/**
 * Skeleton card component (for list items)
 */
export const SkeletonCard: React.FC<SkeletonProps> = ({
  style,
  height = 100,
}) => {
  const progress = useSharedValue(0);

  useEffect(() => {
    progress.value = withRepeat(
      withTiming(1, { duration: 1200 }),
      -1,
      true
    );
  }, []);

  const animatedStyle = useAnimatedStyle(() => {
    const opacity = interpolate(
      progress.value,
      [0, 1],
      [0.3, 0.8],
      Extrapolate.CLAMP
    );
    return {
      opacity,
    };
  });

  return (
    <Animated.View
      style={[
        {
          width: '100%',
          height,
          backgroundColor: AccordDarkTheme.colors.surface,
          borderRadius: 12,
          borderWidth: 1,
          borderColor: AccordDarkTheme.colors.outline,
          marginBottom: Spacing.md,
          padding: Spacing.md,
        },
        style,
        animatedStyle,
      ]}
    >
      <SkeletonLine width="60%" height={16} />
      <SkeletonLine width="80%" height={12} style={{ marginTop: Spacing.sm }} />
      <SkeletonLine width="40%" height={12} style={{ marginTop: Spacing.sm }} />
    </Animated.View>
  );
};

/**
 * Skeleton general component
 */
export const Skeleton: React.FC<SkeletonProps> = ({
  variant = 'line',
  count = 1,
  width = '100%',
  height = 16,
  spacing = Spacing.md,
  style,
}) => {
  if (variant === 'circle') {
    return <SkeletonCircle size={height} style={style} />;
  }

  if (variant === 'card') {
    return <SkeletonCard height={height} style={style} />;
  }

  if (variant === 'line') {
    if (count === 1) {
      return <SkeletonLine width={width} height={height} style={style} />;
    }
    return <SkeletonText width={width} height={height} count={count} spacing={spacing} style={style} />;
  }

  return null;
};

export default Skeleton;
