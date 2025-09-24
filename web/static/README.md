# Static Files

This directory contains the static assets for the Concordance Dataset State Historian web interface.

## Structure

```
static/
├── css/
│   └── main.css          # Main stylesheet with IBM Plex Mono design
├── js/
│   └── main.js           # Main JavaScript functionality
├── images/               # Image assets (currently empty)
└── README.md            # This file
```

## Design System

### Typography
- **Font**: IBM Plex Mono (Google Fonts)
- **Style**: Monospace, clean, professional

### Color Palette
- **Primary**: Grayscale (#1a1a1a to #f8f8f8)
- **Accent**: Green (#00ff00) for highlights
- **Status Colors**:
  - Available: #00aa44
  - Unavailable: #cc0000
  - Error: #ff6600
  - Unknown: #666666

### Layout
- **Grid System**: CSS Grid for responsive layouts
- **Spacing**: Consistent 20px margins and padding
- **Borders**: Clean, no rounded corners
- **Shadows**: Subtle, professional

## File Organization

### CSS (main.css)
- Global styles and resets
- Component styles (cards, tables, buttons)
- Responsive design
- Utility classes
- Status indicators
- Real-time elements

### JavaScript (main.js)
- Socket.IO integration
- API communication
- Data filtering and pagination
- Real-time updates
- Navigation handling
- Utility functions

## Usage

The static files are automatically served by Flask when using `url_for('static', filename='...')` in templates.

Example:
```html
<link rel="stylesheet" href="{{ url_for('static', filename='css/main.css') }}">
<script src="{{ url_for('static', filename='js/main.js') }}"></script>
```

## Maintenance

- Keep CSS organized by component
- Use consistent naming conventions
- Maintain responsive design principles
- Test across different screen sizes
- Ensure accessibility compliance
