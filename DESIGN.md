# Design System — Project Trace / Project Compass

## Color strategy: Restrained

Tinted neutrals + two brand accents. Color carries meaning (status, brand, data) — never decoration.

## Palette (OKLCH)

```css
/* Brand */
--brand:     oklch(32% 0.09 255);   /* Baltimore navy  — was #1B4B82 */
--brand-mid: oklch(52% 0.11 255);   /* Mid blue        — was #3A7CC4 */
--brand-lt:  oklch(95% 0.02 255);   /* Light tint      — was #EBF2FA */

/* Accent */
--accent:    oklch(54% 0.14 35);    /* Baltimore orange — was #D85A30 */
--accent-lt: oklch(96% 0.02 35);    /* Light tint      — was #FAECE7 */

/* Status */
--success:   oklch(58% 0.13 162);   /* Green           — was #1D9E75 */
--success-lt:oklch(96% 0.02 162);   /* Light tint      — was #E1F5EE */
--warn:      oklch(57% 0.11 68);    /* Amber           — was #BA7517 */
--warn-lt:   oklch(97% 0.02 68);    /* Light tint      — was #FAEEDA */

/* Neutrals — all tinted navy */
--bg:        oklch(99% 0.005 255);  /* Page background */
--bg2:       oklch(97% 0.007 255);  /* Card / sidebar bg */
--bg3:       oklch(94% 0.008 255);  /* Pressed / track bg */
--border:    oklch(88% 0.01 255);   /* Borders */
--txt:       oklch(20% 0.015 255);  /* Primary text */
--txt2:      oklch(42% 0.012 255);  /* Secondary text */
--txt3:      oklch(58% 0.009 255);  /* Tertiary / labels */
```

## Data palette (choropleth + bars)
5-stop diverging: red → amber → neutral blue → green
```
low:    oklch(42% 0.16 28)   /* #A32D2D */
lo-mid: oklch(60% 0.14 35)   /* #D85A30 */
mid:    oklch(68% 0.12 68)   /* #EF9F27 */
hi-mid: oklch(56% 0.10 230)  /* #3A7CC4 */
high:   oklch(58% 0.13 162)  /* #1D9E75 */
```

## Typography

- **UI font:** Inter, system-ui (already loaded)
- **Mono:** ui-monospace, SFMono-Regular (for IDs, code, URLs)
- **Scale:** 9px (labels) → 10px (meta) → 11px (small UI) → 12px (secondary) → 13px (body) → 14px (emphasis) → 17px (section heads) → 20px (panel heads)
- **Hierarchy rule:** ≥1.3× ratio between adjacent steps; weight contrast (400 → 500) at each step
- **Line length:** 65–75ch max on any reading text
- Body text: `line-height: 1.5`; heading: `line-height: 1.2`

## Spacing rhythm

Not uniform. Use intentional variation:
- Tight (4–6px): between icon and label, badge padding
- Standard (8–12px): card internal padding, form gaps
- Relaxed (16–20px): section gaps, panel padding
- Breathing (24–32px): major section separators

## Motion

- Duration: 120ms (micro) / 200ms (standard) / 400ms (data transitions)
- Easing: `cubic-bezier(0.16, 1, 0.3, 1)` (expo-out) for entrances; `ease-out` for data bars
- Animate only: `transform`, `opacity`, `color`, `background-color`, `border-color`, `width` (data bars only)
- Never: `transition: all`, layout properties, `transition: height`
- Always: `@media (prefers-reduced-motion: reduce)` disables all animations

## Component patterns

**Metric cards:** Small, dense, no shadow. Background tint (--bg2). Value in 500 weight. Delta in 11px with color coding.

**Data bars:** 8px height, rounded track, brand-colored fill. Transition: `width 0.4s cubic-bezier(0.16,1,0.3,1)`. Label left-aligned at fixed width.

**Status badges:** Pill, 11px, 500 weight. Always paired with a colored background (success-lt / warn-lt / accent-lt / brand-lt).

**Tabs:** No underline style. Active = brand background + white text. Hover = bg2 tint. 0.5px border. Border-radius 4–6px.

**Annotations (Assumptive layer):** Dotted 1px border (--brand at 40% opacity), bg2 background. Label "Human insight" in 9px warn color. Never side-stripe.

**Gap View indicators:** Full-border card, not side-stripe. Divergence color fills the card header band (full width, 4px height at top).

## What not to do

- No side-stripe borders (`border-left > 1px` as accent) — ever
- No gradient text
- No box shadows on cards (use border instead)
- No hero-metric template (big number + gradient card)
- No `transition: all`
- No `outline: none` without `:focus-visible` replacement
- No hardcoded `white` or `#000/#fff` — always use palette tokens
