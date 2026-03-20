/**
 * Accord Paper Components Library
 * 
 * High-level components that wrap react-native-paper
 * with Accord's design system pre-applied
 */

import React from 'react';
import { StyleProp, ViewStyle, TextStyle } from 'react-native';
import {
  Button as PaperButton,
  ButtonProps as PaperButtonProps,
  Card as PaperCard,
  CardProps,
  TextInput as PaperTextInput,
  TextInputProps,
  Text as PaperText,
  TextProps,
} from 'react-native-paper';
import { AccordDarkTheme, ComponentSize, Spacing } from '../AccordTheme';

// ============================================================================
// BUTTONS
// ============================================================================

interface AccordButtonProps extends Omit<PaperButtonProps, 'children'> {
  children: string;
  size?: 'small' | 'medium' | 'large';
  variant?: 'filled' | 'outlined' | 'text';
  isWarning?: boolean;
  isLoading?: boolean;
}

export const PrimaryButton: React.FC<AccordButtonProps> = ({
  children,
  size = 'medium',
  variant = 'filled',
  isWarning = false,
  disabled,
  style,
  ...props
}) => {
  const height = {
    small: ComponentSize.button.small,
    medium: ComponentSize.button.medium,
    large: ComponentSize.button.large,
  }[size];

  const backgroundColor = isWarning
    ? AccordDarkTheme.colors.error
    : AccordDarkTheme.colors.primary;

  return (
    <PaperButton
      mode={variant}
      buttonColor={backgroundColor}
      disabled={disabled}
      style={[{ height }, style as StyleProp<ViewStyle>]}
      labelStyle={{ fontSize: 14, fontWeight: '700' }}
      {...props}
    >
      {children}
    </PaperButton>
  );
};

export const SecondaryButton: React.FC<AccordButtonProps> = ({
  children,
  size = 'medium',
  variant = 'outlined',
  style,
  ...props
}) => {
  const height = {
    small: ComponentSize.button.small,
    medium: ComponentSize.button.medium,
    large: ComponentSize.button.large,
  }[size];

  return (
    <PaperButton
      mode={variant}
      buttonColor={AccordDarkTheme.colors.secondary}
      style={[{ height }, style as StyleProp<ViewStyle>]}
      labelStyle={{ fontSize: 14, fontWeight: '700' }}
      {...props}
    >
      {children}
    </PaperButton>
  );
};

// ============================================================================
// CARDS
// ============================================================================

interface AccordCardProps extends CardProps {
  title?: string;
  subtitle?: string;
  children?: React.ReactNode;
}

export const PremiumCard: React.FC<AccordCardProps> = ({
  title,
  subtitle,
  children,
  style,
  ...props
}) => {
  return (
    <PaperCard
      style={[
        {
          backgroundColor: AccordDarkTheme.colors.surface,
          borderColor: AccordDarkTheme.colors.outline,
          borderWidth: 1,
          borderRadius: 12,
        },
        style as StyleProp<ViewStyle>,
      ]}
      {...props}
    >
      {(title || subtitle) && (
        <PaperCard.Title title={title} subtitle={subtitle} />
      )}
      {children && <PaperCard.Content>{children}</PaperCard.Content>}
    </PaperCard>
  );
};

// ============================================================================
// TEXT INPUT
// ============================================================================

interface AccordTextInputProps extends TextInputProps {
  label?: string;
  error?: string;
  size?: 'small' | 'medium';
}

export const PremiumTextInput: React.FC<AccordTextInputProps> = ({
  label,
  error,
  size = 'medium',
  style,
  ...props
}) => {
  return (
    <PaperTextInput
      label={label}
      mode="outlined"
      outlineColor={AccordDarkTheme.colors.outline}
      activeOutlineColor={AccordDarkTheme.colors.primary}
      textColor={AccordDarkTheme.colors.onSurface}
      style={[{ height: ComponentSize.input }, style as StyleProp<ViewStyle>]}
      error={!!error}
      placeholderTextColor={AccordDarkTheme.colors.outlineVariant}
      {...props}
    />
  );
};

// ============================================================================
// TEXT COMPONENTS
// ============================================================================

interface AccordTextProps extends TextProps {
  variant?: 'displayLarge' | 'headlineLarge' | 'titleLarge' | 'bodyLarge' | 'labelLarge';
  color?: string;
  bold?: boolean;
}

export const AccordText: React.FC<AccordTextProps> = ({
  variant = 'bodyLarge',
  color = AccordDarkTheme.colors.onSurface,
  bold = false,
  style,
  children,
  ...props
}) => {
  const variantStyles: Record<string, TextStyle> = {
    displayLarge: { fontSize: 32, lineHeight: 40, fontWeight: bold ? '700' : '400' },
    headlineLarge: { fontSize: 24, lineHeight: 32, fontWeight: bold ? '700' : '400' },
    titleLarge: { fontSize: 22, lineHeight: 28, fontWeight: bold ? '700' : '500' },
    bodyLarge: { fontSize: 16, lineHeight: 24, fontWeight: bold ? '700' : '400' },
    labelLarge: { fontSize: 14, lineHeight: 20, fontWeight: '700' },
  };

  return (
    <PaperText
      style={[variantStyles[variant], { color }, style as StyleProp<TextStyle>]}
      {...props}
    >
      {children}
    </PaperText>
  );
};

export const Heading: React.FC<Omit<AccordTextProps, 'variant'>> = (props) => (
  <AccordText variant="headlineLarge" bold {...props} />
);

export const Subheading: React.FC<Omit<AccordTextProps, 'variant'>> = (props) => (
  <AccordText variant="titleLarge" bold {...props} />
);

export const Caption: React.FC<Omit<AccordTextProps, 'variant'>> = (props) => (
  <AccordText variant="labelLarge" color={AccordDarkTheme.colors.outlineVariant} {...props} />
);

// ============================================================================
// BADGES / PILLS
// ============================================================================

interface BadgeProps {
  label: string;
  variant?: 'success' | 'warning' | 'error' | 'info';
  size?: 'small' | 'medium';
}

export const Badge: React.FC<BadgeProps> = ({
  label,
  variant = 'info',
  size = 'medium',
}) => {
  const colors = {
    success: { bg: AccordDarkTheme.colors.tertiary, text: '#ffffff' },
    warning: { bg: AccordDarkTheme.colors.warning, text: '#000000' },
    error: { bg: AccordDarkTheme.colors.error, text: '#ffffff' },
    info: { bg: AccordDarkTheme.colors.primary, text: '#ffffff' },
  };

  const color = colors[variant];
  const padding = size === 'small' ? Spacing.sm : Spacing.md;

  return (
    <PaperText
      style={{
        backgroundColor: color.bg,
        color: color.text,
        paddingHorizontal: padding,
        paddingVertical: padding / 2,
        borderRadius: 16,
        fontSize: 12,
        fontWeight: '600',
        alignSelf: 'flex-start',
      }}
    >
      {label}
    </PaperText>
  );
};

export default {
  PrimaryButton,
  SecondaryButton,
  PremiumCard,
  PremiumTextInput,
  AccordText,
  Heading,
  Subheading,
  Caption,
  Badge,
};
