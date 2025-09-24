# CSS Architecture

This directory contains the organized CSS architecture for the PKL Dashboard.

## File Structure

```
assets/css/
├── README.md          # This file - CSS architecture documentation
├── critical.css       # Critical CSS for initial page load and FOUC prevention
├── variables.css      # CSS custom properties and design tokens
└── main.css          # Main application styles
```

## CSS Files Overview

### 1. `critical.css`
**Purpose**: Critical CSS loaded first to prevent Flash of Unstyled Content (FOUC)

**Contains**:
- Base body styles
- Loading overlay and spinner
- Dashboard opacity transitions
- Initial visibility controls

**Loading**: Loaded first with `onload` callback to track stylesheet loading

### 2. `variables.css`
**Purpose**: CSS custom properties and design system tokens

**Contains**:
- Color palette (primary, secondary, backgrounds, text)
- Typography scale (font sizes, weights, line heights)
- Spacing system (consistent spacing units)
- Border radius values
- Shadow definitions
- Z-index layers
- Transition timing

**Usage**: Variables are used throughout `main.css` for consistency

### 3. `main.css`
**Purpose**: Main application styles organized by component

**Sections**:
- Reset and base styles
- Layout (container, header, main content)
- Stats grid and cards
- Charts and visualizations
- View controls and navigation
- Session lists and items
- Buttons and form elements
- Modals and overlays
- Notebook grid
- Conversation displays
- Live duration indicators
- Empty states and error handling
- Responsive design (media queries)

## Design Principles

### 1. **No Inline Styles**
- All styles are externalized to CSS files
- JavaScript uses CSS classes for state changes
- Dynamic styles use predefined CSS classes

### 2. **CSS Custom Properties**
```css
/* Design tokens in variables.css */
--primary-color: #007AFF;
--space-4: 16px;
--font-size-lg: 18px;

/* Usage in main.css */
.button {
  background: var(--primary-color);
  padding: var(--space-4);
  font-size: var(--font-size-lg);
}
```

### 3. **Component-Based Organization**
- Styles grouped by UI component
- Clear section comments
- Logical cascade and specificity

### 4. **State Management**
```css
/* Button states via CSS classes */
.btn.btn-success { background: #34c759; }
.btn.btn-error { background: #ff3b30; }
.btn.btn-loading { opacity: 0.7; cursor: wait; }
```

### 5. **Performance Optimization**
- Critical CSS loaded first
- Non-critical CSS loaded with callbacks
- Minimal initial render blocking

## Loading Strategy

1. **Critical CSS**: Immediate load for FOUC prevention
2. **Variables CSS**: Design tokens loaded second
3. **Main CSS**: Full styles loaded third
4. **Loading Tracking**: JavaScript tracks when all stylesheets load
5. **Visibility Control**: Body shown only after stylesheets load

## Responsive Design

- Mobile-first approach
- Breakpoint: 768px for tablet/desktop
- Grid layouts adapt to screen size
- Typography scales appropriately

## Maintenance

### Adding New Styles
1. Check if CSS custom properties exist in `variables.css`
2. Add new component styles to appropriate section in `main.css`
3. Use existing design tokens for consistency
4. Follow BEM-like naming conventions

### Modifying Existing Styles
1. Locate styles in appropriate CSS file
2. Consider impact on other components
3. Test across different screen sizes
4. Maintain design system consistency

## Browser Support

- Modern browsers (Chrome, Firefox, Safari, Edge)
- CSS Grid and Flexbox
- CSS Custom Properties
- CSS Animations and Transitions
