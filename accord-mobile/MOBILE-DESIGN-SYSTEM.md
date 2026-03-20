# Accord Mobile UI/UX System

## Overview

The Accord mobile app now features a **professional Material Design 3** system powered by React Native Paper, with:

- **Premium Dark Theme**: Carefully crafted color palette for fintech credibility
- **Fluid Animations**: Smooth transitions and interactions using React Native Reanimated
- **Skeleton Loaders**: Perceived performance improvements during data fetching
- **Responsive Layout**: Tablet-ready grid layouts
- **Accessibility**: Reduced motion mode support, WCAG color contrast compliance

---

## Design Tokens

### Color Palette

All colors are defined in [`AccordTheme.ts`](./AccordTheme.ts):

#### Primary Colors (Brand)
```
Primary Blue: #2d6cdf (main brand color)
  - Used for: Buttons, links, highlights
  - Accessible: AA contrast on all backgrounds

Secondary (Accent): #4a9eff (bright blue for CTAs)
  - Used for: Secondary actions, highlights

Tertiary (Success): #10b981 (green for positive actions)
Tertiary Container: #0f766e (darker green for backgrounds)
```

#### Semantic Colors
```
Success:  #10b981  (transactions, approvals)
Warning:  #f59e0b  (alerts, caution)
Error:    #ef4444  (destructive, failures)
Info:     #3b82f6  (informational)
Pending:  #8b5cf6  (processing states)
```

#### Neutral Scale (Dark Theme)
```
Background:  #0b1220  (main app background, deepest)
Surface:     #131d30  (cards, panels)
Variant:     #1e2742  (hover states, secondary surfaces)

Text/Foreground:
- onSurface:     #d2dff8  (primary text on surfaces)
- onBackground:  #e3ecff  (secondary text)
- onPrimary:     #ffffff  (text on primary color)
- outline:       #3e5f92  (borders, dividers)
```

### Typography Scale

Implemented in Material Design 3 with 11 levels:

```
Display Large   32px / 700 weight  (hero titles)
Display Medium  28px / 700 weight
Display Small   24px / 700 weight

Headline Large  24px / 700 weight  (page titles)
Headline Medium 20px / 700 weight
Headline Small  18px / 700 weight

Title Large     22px / 700 weight  (section headers)
Title Medium    16px / 700 weight
Title Small     14px / 700 weight

Body Large      16px / 400 weight  (primary text)
Body Medium     14px / 400 weight  (secondary text)
Body Small      12px / 400 weight  (captions)

Label Large     14px / 700 weight  (buttons, badges)
Label Medium    12px / 700 weight
Label Small     11px / 700 weight  (helpers)
```

### Spacing Scale (8pt Grid)

```
xs:   4px   (tight spacing, micro-interactions)
sm:   8px   (default spacing)
md:  12px   (component spacing)
lg:  16px   (section spacing)
xl:  24px   (major spacing)
xxl: 32px   (large sections)
```

---

## Core Components

### Buttons

#### Primary Button
```tsx
import { PrimaryButton } from './components/AccordComponents';

<PrimaryButton onPress={() => {}}>
  Submit Payment
</PrimaryButton>

// Sizes
<PrimaryButton size="small">Small</PrimaryButton>
<PrimaryButton size="medium">Medium</PrimaryButton>
<PrimaryButton size="large">Large</PrimaryButton>

// Variants
<PrimaryButton variant="filled">Filled (default)</PrimaryButton>
<PrimaryButton variant="outlined">Outlined</PrimaryButton>
<PrimaryButton variant="text">Text Only</PrimaryButton>

// States
<PrimaryButton isLoading>Loading...</PrimaryButton>
<PrimaryButton disabled>Disabled</PrimaryButton>
<PrimaryButton isWarning>Destructive Action</PrimaryButton>
```

#### Secondary Button
```tsx
<SecondaryButton onPress={() => {}}>Cancel</SecondaryButton>
```

### Cards

```tsx
import { PremiumCard } from './components/AccordComponents';

<PremiumCard title="Transaction Details" subtitle="GST-2026-001">
  <AccordText>Content goes here</AccordText>
</PremiumCard>
```

### Text Inputs

```tsx
import { PremiumTextInput } from './components/AccordComponents';

<PremiumTextInput
  label="Filing ID"
  placeholder="e.g., 1001"
  value={filingId}
  onChangeText={setFilingId}
  error={errorMessage}
/>
```

### Text Components

```tsx
import { Heading, Subheading, Caption, AccordText } from './components/AccordComponents';

<Heading>Page Title</Heading>
<Subheading color="#4a9eff">Section Header</Subheading>
<AccordText variant="bodyLarge">Regular text content</AccordText>
<Caption>Helper text or metadata</Caption>
```

### Badges

```tsx
import { Badge } from './components/AccordComponents';

<Badge label="APPROVED" variant="success" />
<Badge label="PENDING" variant="info" />
<Badge label="FAILED" variant="error" size="small" />
```

---

## Animations

All animations are defined in [`hooks/useVoiceAnimations.ts`](./hooks/useVoiceAnimations.ts).

### Voice Recording - Pulsing Microphone

```tsx
import { usePulsingAnimation } from './hooks/useVoiceAnimations';
import Animated from 'react-native-reanimated';

export function VoiceButton({ isListening }) {
  const pulseStyle = usePulsingAnimation(isListening);

  return (
    <Animated.View style={pulseStyle}>
      <MicrophoneIcon />
    </Animated.View>
  );
}
```

Result: Microphone scales 1→1.2→1 and fades 1→0.6→1 while listening.

### Sound Wave Animation

```tsx
import { useWaveAnimation } from './hooks/useVoiceAnimations';
import Animated from 'react-native-reanimated';

export function SoundWaves({ isListening }) {
  const wave1 = useWaveAnimation(isListening, 0);
  const wave2 = useWaveAnimation(isListening, 1);
  const wave3 = useWaveAnimation(isListening, 2);

  return (
    <>
      <Animated.View style={wave1}>
        <Circle />
      </Animated.View>
      <Animated.View style={wave2}>
        <Circle />
      </Animated.View>
      <Animated.View style={wave3}>
        <Circle />
      </Animated.View>
    </>
  );
}
```

Result: Concentric circles expand and fade, creating ripple effect.

### Transcription Fade-In

```tsx
import { useFadeAnimation } from './hooks/useVoiceAnimations';
import Animated from 'react-native-reanimated';

export function LiveTranscript({ transcript }) {
  const fadeStyle = useFadeAnimation(!!transcript);

  return (
    <Animated.View style={fadeStyle}>
      <Text>{transcript}</Text>
    </Animated.View>
  );
}
```

### Loading Spinner

```tsx
import { useSpinAnimation } from './hooks/useVoiceAnimations';
import Animated from 'react-native-reanimated';

export function ProcessingSpinner({ isProcessing }) {
  const spinStyle = useSpinAnimation(isProcessing);

  return (
    <Animated.View style={spinStyle}>
      <LoadingIcon />
    </Animated.View>
  );
}
```

### Slide-Up Modal

```tsx
import { useSlideUpAnimation } from './hooks/useVoiceAnimations';
import Animated from 'react-native-reanimated';

export function BottomSheet({ visible }) {
  const slideStyle = useSlideUpAnimation(visible);

  return (
    <Animated.View style={slideStyle}>
      {/* Bottom sheet content */}
    </Animated.View>
  );
}
```

---

## Skeleton Loaders

Display placeholder content while data loads, improving perceived performance.

```tsx
import { Skeleton, SkeletonLine, SkeletonCard, SkeletonText } from './components/SkeletonLoader';

// Single line
{isLoading && <SkeletonLine width="60%" height={16} />}

// Multiple lines of text
{isLoading && <SkeletonText count={3} />}

// Card placeholder
{isLoading && <SkeletonCard height={120} />}

// Avatar
{isLoading && <Skeleton variant="circle" width={40} height={40} />}

// Flexible variant
{isLoading && <Skeleton variant="card" count={5} height={80} />}
```

### Example: Transaction List with Skeleton

```tsx
export function TransactionList({ transactions, isLoading }) {
  if (isLoading) {
    return (
      <View>
        <SkeletonCard height={100} />
        <SkeletonCard height={100} />
        <SkeletonCard height={100} />
      </View>
    );
  }

  return (
    <View>
      {transactions.map(txn => (
        <PremiumCard key={txn.id} title={txn.reference}>
          <AccordText>{txn.amount}</AccordText>
        </PremiumCard>
      ))}
    </View>
  );
}
```

---

## Dark Mode Support

The app uses Material Design 3 dark theme by default (`AccordDarkTheme`).

### Status Bar

```tsx
<StatusBar 
  barStyle="light-content" 
  backgroundColor={AccordDarkTheme.colors.background}
/>
```

### Custom Theme Override (Future)

To implement light mode or custom themes:

```tsx
import { MD3LightTheme } from 'react-native-paper';
import { PaperProvider } from 'react-native-paper';

const customTheme = {
  ...MD3LightTheme,
  colors: { /* your colors */ }
};

<PaperProvider theme={customTheme}>
  {/* App content */}
</PaperProvider>
```

---

## Migration Guide: From Old to New Components

### Before (Raw React Native)
```tsx
<TouchableOpacity 
  style={{
    backgroundColor: "#2d6cdf",
    borderRadius: 8,
    paddingVertical: 10
  }}
  onPress={handleSubmit}
>
  <Text style={{ color: "#fff", fontWeight: "700" }}>Submit</Text>
</TouchableOpacity>

<TextInput
  style={{
    backgroundColor: "#0c1527",
    borderColor: "#31486f",
    color: "#e3ecff",
    paddingHorizontal: 10,
    paddingVertical: 8
  }}
  placeholder="Enter ID"
  placeholderTextColor="#8ca0c0"
/>
```

### After (Accord Design System)
```tsx
<PrimaryButton onPress={handleSubmit}>Submit</PrimaryButton>

<PremiumTextInput
  label="Enter ID"
  value={id}
  onChangeText={setId}
/>
```

Benefits:
- 70% less code
- Consistent styling across app
- Theme changes automatically applied
- Accessibility built-in
- Animations ready to use

---

## Responsive Design

### Tablet Support

```tsx
import { useWindowDimensions } from 'react-native';

export function ResponsiveLayout() {
  const { width } = useWindowDimensions();
  const isTablet = width >= 900;
  const columns = isTablet ? 2 : 1;

  return (
    <View style={{ flexDirection: 'row', flexWrap: 'wrap' }}>
      {items.map(item => (
        <View key={item.id} style={{ width: `${100 / columns}%` }}>
          <PremiumCard>{item.name}</PremiumCard>
        </View>
      ))}
    </View>
  );
}
```

---

## Best Practices

### 1. Use Theme Colors, Never Hardcode

```tsx
// ❌ Bad
<View style={{ backgroundColor: "#131d30" }} />

// ✅ Good
import { AccordDarkTheme } from './AccordTheme';
<View style={{ backgroundColor: AccordDarkTheme.colors.surface }} />
```

### 2. Use Spacing Scale

```tsx
// ❌ Bad
paddingHorizontal: 13

// ✅ Good
paddingHorizontal: Spacing.md  // 12px
```

### 3. Use Typography Scale

```tsx
// ❌ Bad
fontSize: 15, fontWeight: "500"

// ✅ Good
<AccordText variant="bodyLarge">Content</AccordText>
```

### 4. Lazy-Load Skeleton Content

```tsx
// ✅ Good
{isLoading ? <SkeletonCard /> : <DataCard data={data} />}
```

### 5. Animate to Delight, Not Distract

```tsx
// ✅ Use for critical UX flows
usePulsingAnimation(isListening)   // Voice recording
useWaveAnimation(isListening)      // Sound visualization

// ❌ Avoid: Unnecessary animations on every interaction
```

---

## Performance Considerations

### Animation Performance

React Native Reanimated runs on the native thread, avoiding JavaScript bridge.

```tsx
// ✅ Good: Uses native thread
const pulseStyle = usePulsingAnimation(isActive);

// ❌ Avoid: JavaScript calculations per frame
const [scale, setScale] = useState(1);
useEffect(() => {
  const interval = setInterval(() => setScale(s => s + 0.01), 16);
  return () => clearInterval(interval);
}, []);
```

### Skeleton Optimization

```tsx
// ✅ Pre-render placeholders
{isLoading && <SkeletonCard />}

// ❌ Avoid: Complex data rendering then replacing
{isLoading && <ComplexLayout /> && !isLoading && <RealLayout />}
```

---

## Testing Components

```tsx
import { render } from '@testing-library/react-native';
import { PaperProvider } from 'react-native-paper';
import { AccordDarkTheme } from './AccordTheme';

const renderWithTheme = (component) => {
  return render(
    <PaperProvider theme={AccordDarkTheme}>
      {component}
    </PaperProvider>
  );
};

test('PrimaryButton renders', () => {
  const { getByText } = renderWithTheme(
    <PrimaryButton>Click Me</PrimaryButton>
  );
  expect(getByText('Click Me')).toBeTruthy();
});
```

---

## Reference Links

- [React Native Paper Docs](https://callstack.github.io/react-native-paper/)
- [React Native Reanimated Docs](https://docs.swmansion.com/react-native-reanimated/)
- [Material Design 3 Guidelines](https://m3.material.io/)
- [Accordion Theme File](./AccordTheme.ts)
- [Animations Hook](./hooks/useVoiceAnimations.ts)
- [Skeleton Loader](./components/SkeletonLoader.tsx)
- [Accord Components](./components/AccordComponents.tsx)

---

## Next Steps

1. **Incrementally migrate** existing components to use AccordComponents
2. **Add dark mode toggle** (future feature)
3. **Implement navigation animations** (Expo Router + Reanimated)
4. **Add haptic feedback** (expo-haptics on button presses)
5. **Expand component library** (modals, sheets, tabs)
