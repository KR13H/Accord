/**
 * Accord Premium Design System
 * 
 * Custom Material Design 3 theme for Accord mobile app
 * Implements dark-first design with premium fintech aesthetic
 * Supports accessibility and motion-safe mode
 */

import { MD3DarkTheme } from 'react-native-paper';

export const AccordDarkTheme = {
  ...MD3DarkTheme,
  colors: {
    ...MD3DarkTheme.colors,
    // Primary brand color (deep blue)
    primary: '#2d6cdf',
    primaryContainer: '#1e4796',
    onPrimary: '#ffffff',
    onPrimaryContainer: '#e8f0ff',

    // Secondary (accent for CTAs)
    secondary: '#4a9eff',
    secondaryContainer: '#0d47a1',
    onSecondary: '#ffffff',
    onSecondaryContainer: '#e3f2fd',

    // Tertiary (success/positive actions)
    tertiary: '#10b981',
    tertiaryContainer: '#0f766e',
    onTertiary: '#ffffff',
    onTertiaryContainer: '#d1fae5',

    // Error (destructive actions, warnings)
    error: '#ef4444',
    errorContainer: '#7f1d1d',
    onError: '#ffffff',
    onErrorContainer: '#fee2e2',

    // Neutral backgrounds (dark theme)
    background: '#0b1220',        // Main background
    surface: '#131d30',            // Cards, panels
    surfaceVariant: '#1e2742',    // Hover states
    inverseOnSurface: '#0b1220',
    inverseSurface: '#e8ecf1',
    scrim: 'rgba(0, 0, 0, 0.5)',

    // Text colors
    onBackground: '#e3ecff',
    onSurface: '#d2dff8',
    outline: '#3e5f92',
    outlineVariant: '#516b8d',

    // Additional semantic colors
    success: '#10b981',
    warning: '#f59e0b',
    info: '#3b82f6',
    pending: '#8b5cf6',
    disabled: '#4b5563',
    
    // Premium surfaces
    elevation: {
      level0: 'transparent',
      level1: '#1a2839',
      level2: '#202e45',
      level3: '#273954',
      level4: '#2b3c5d',
      level5: '#313d66',
    },
  },
};

/**
 * Typography Scale
 * Implements Material Design 3 typography with fintech clarity
 */
export const TypographyScale = {
  displayLarge: {
    fontSize: 57,
    lineHeight: 64,
    fontWeight: '700' as const,
    letterSpacing: -0.25,
  },
  displayMedium: {
    fontSize: 45,
    lineHeight: 52,
    fontWeight: '700' as const,
    letterSpacing: 0,
  },
  displaySmall: {
    fontSize: 36,
    lineHeight: 44,
    fontWeight: '700' as const,
    letterSpacing: 0,
  },
  headlineLarge: {
    fontSize: 32,
    lineHeight: 40,
    fontWeight: '700' as const,
    letterSpacing: 0,
  },
  headlineMedium: {
    fontSize: 28,
    lineHeight: 36,
    fontWeight: '700' as const,
    letterSpacing: 0,
  },
  headlineSmall: {
    fontSize: 24,
    lineHeight: 32,
    fontWeight: '700' as const,
    letterSpacing: 0,
  },
  titleLarge: {
    fontSize: 22,
    lineHeight: 28,
    fontWeight: '700' as const,
    letterSpacing: 0,
  },
  titleMedium: {
    fontSize: 16,
    lineHeight: 24,
    fontWeight: '700' as const,
    letterSpacing: 0.15,
  },
  titleSmall: {
    fontSize: 14,
    lineHeight: 20,
    fontWeight: '700' as const,
    letterSpacing: 0.1,
  },
  bodyLarge: {
    fontSize: 16,
    lineHeight: 24,
    fontWeight: '400' as const,
    letterSpacing: 0.15,
  },
  bodyMedium: {
    fontSize: 14,
    lineHeight: 20,
    fontWeight: '400' as const,
    letterSpacing: 0.25,
  },
  bodySmall: {
    fontSize: 12,
    lineHeight: 16,
    fontWeight: '400' as const,
    letterSpacing: 0.4,
  },
  labelLarge: {
    fontSize: 14,
    lineHeight: 20,
    fontWeight: '500' as const,
    letterSpacing: 0.1,
  },
  labelMedium: {
    fontSize: 12,
    lineHeight: 16,
    fontWeight: '500' as const,
    letterSpacing: 0.5,
  },
  labelSmall: {
    fontSize: 11,
    lineHeight: 16,
    fontWeight: '500' as const,
    letterSpacing: 0.5,
  },
};

/**
 * Spacing Scale (8pt grid system)
 */
export const Spacing = {
  xs: 4,
  sm: 8,
  md: 12,
  lg: 16,
  xl: 24,
  xxl: 32,
  xxxl: 48,
};

/**
 * Elevation (Shadow) Scale
 * Provides consistent depth perception
 */
export const Elevations = {
  none: {
    elevation: 0,
    shadowColor: 'transparent',
  },
  sm: {
    elevation: 2,
    shadowColor: '#000000',
    shadowOffset: { width: 0, height: 1 },
    shadowOpacity: 0.18,
    shadowRadius: 1.0,
  },
  md: {
    elevation: 4,
    shadowColor: '#000000',
    shadowOffset: { width: 0, height: 2 },
    shadowOpacity: 0.20,
    shadowRadius: 1.41,
  },
  lg: {
    elevation: 8,
    shadowColor: '#000000',
    shadowOffset: { width: 0, height: 4 },
    shadowOpacity: 0.25,
    shadowRadius: 3.84,
  },
  xl: {
    elevation: 12,
    shadowColor: '#000000',
    shadowOffset: { width: 0, height: 8 },
    shadowOpacity: 0.30,
    shadowRadius: 4.65,
  },
};

/**
 * Border Radius Scale
 */
export const BorderRadius = {
  none: 0,
  sm: 4,
  md: 8,
  lg: 12,
  xl: 16,
  full: 9999,
};

/**
 * Animation Presets
 */
export const AnimationDurations = {
  fast: 150,
  default: 300,
  slow: 500,
  verySlow: 800,
};

/**
 * Component-specific spacing and sizing
 */
export const ComponentSize = {
  // Button sizes
  button: {
    small: 36,
    medium: 44,
    large: 52,
  },
  // Icon sizes
  icon: {
    small: 16,
    medium: 24,
    large: 32,
    xlarge: 48,
  },
  // Input field height
  input: 44,
  // Card radius
  card: 12,
};

export default AccordDarkTheme;
