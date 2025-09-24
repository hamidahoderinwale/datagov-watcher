# Live PKL Dashboard

A comprehensive real-time monitoring system for Cursor IDE sessions and notebook changes, providing detailed insights into coding patterns, AI interactions, and development workflows.

## Features

### Core Functionality
- **Real-time Session Tracking**: Monitor active coding sessions in Cursor IDE
- **Conversation Capture**: Track AI interactions and prompts automatically
- **Code Change Analysis**: Detailed tracking of code deltas and file modifications
- **Intent Classification**: Categorize sessions by purpose (Explore, Implement, Debug)
- **Return to Context**: Open files in Cursor IDE and restore cursor position from any session
- **Export Capabilities**: Export session data in multiple formats (JSON, Markdown, CSV)

### Dashboard Features
- **Live Statistics**: Real-time metrics on active sessions, changes, and intents
- **Session History**: Comprehensive view of all coding sessions
- **Notebook Grid View**: Organize sessions by notebook files
- **Conversation Timeline**: Complete chat history with AI assistant
- **Visual Analytics**: Charts and graphs for session patterns

## Architecture

### Project Structure
```
cursor-pkl-extension/
├── assets/
│   ├── css/
│   │   ├── variables.css      # CSS custom properties
│   │   └── main.css          # Main stylesheet
│   └── js/
│       └── dashboard.js      # Dashboard functionality
├── app/
│   ├── services/
│   │   ├── cursor-db-parser.ts    # Cursor database integration
│   │   ├── file-monitor.ts        # File system monitoring
│   │   └── applescript-service.ts # macOS integration
│   ├── storage/
│   │   └── json-manager.ts        # Data persistence
│   └── config/
│       └── types.ts               # TypeScript definitions
├── components/
│   └── empty-state.html           # Reusable components
├── dist/                          # Compiled JavaScript
├── live-dashboard-clean.html      # Main dashboard interface
├── web-server.js                  # Express server
├── data-storage.js                # Data management
└── package.json
```

### Technology Stack
- **Frontend**: HTML5, CSS3, JavaScript (ES6+)
- **Backend**: Node.js, Express.js
- **Database**: JSON-based storage with SQLite integration
- **Monitoring**: File system watchers, Cursor IDE integration
- **Styling**: CSS Custom Properties, Modern CSS Grid/Flexbox

## Installation

### Prerequisites
- Node.js 16+ 
- Cursor IDE
- macOS (for AppleScript integration)

### Setup
1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd cursor-pkl-extension
   ```

2. **Install dependencies**
   ```bash
   npm install
   ```

3. **Compile TypeScript**
   ```bash
   npm run build
   ```

4. **Start the server**
   ```bash
   npm start
   ```

5. **Access the dashboard**
   Open http://localhost:3000 in your browser

## Usage

### Starting a Session
1. Open a `.ipynb` file in Cursor IDE
2. The system automatically detects and starts tracking
3. View real-time updates in the dashboard

### Viewing Session Data
- **Sessions View**: Detailed list of all sessions with metadata
- **Notebook Grid**: Sessions organized by notebook file
- **Session Details**: Click any session to view full details including conversations

### Context Restoration
- **Return to Context**: Click any session's "Return to Context" button
- Automatically opens the file in Cursor IDE at the exact cursor position
- Restores the working context from when the session was active
- Works with both active and historical sessions

### Exporting Data
- Use the export functionality to download session data
- Choose from JSON, Markdown, or CSV formats
- Include/exclude specific data types (conversations, code deltas, etc.)

## API Reference

### Endpoints

#### Sessions
- `GET /api/sessions` - Get all sessions
- `GET /api/session/:id` - Get specific session details
- `GET /api/session/:id/conversations` - Get conversations for a session

#### Conversations
- `GET /api/conversations` - Get all conversations
- `POST /api/conversations` - Create new conversation
- `GET /api/session/:id/conversations` - Get session conversations

#### Exports
- `GET /api/export` - Create new export
- `GET /api/export/list` - List available exports
- `GET /api/export/download/:filename` - Download export file

### Data Models

#### Session
```typescript
interface PKLSession {
  id: string;
  timestamp: Date;
  intent: 'EXPLORE' | 'IMPLEMENT' | 'DEBUG' | string;
  phase: 'IN_PROGRESS' | 'COMPLETED' | 'FAILED';
  outcome: 'IN_PROGRESS' | 'COMPLETED' | 'FAILED';
  confidence: number;
  currentFile: string;
  cursorPosition: { line: number; character: number };
  selectedText: string;
  fileChanges: FileChange[];
  codeDeltas: CodeDelta[];
  linkedEvents: string[];
  privacyMode: boolean;
  userConsent: boolean;
  dataRetention: string;
  annotations: Annotation[];
  executionStates: ExecutionStates;
  semanticAnalysis: SemanticAnalysis;
}
```

#### Conversation
```typescript
interface ConversationEvent {
  id: string;
  sessionId: string;
  timestamp: Date;
  role: 'user' | 'assistant';
  content: string;
  metadata: {
    conversationId: string;
    messageId: string;
    fileContext?: string;
    cellIndex?: number;
  };
  referencedFiles: string[];
  codeBlocks: CodeBlock[];
}
```

## Configuration

### Environment Variables
- `PORT`: Server port (default: 3000)
- `DATA_DIR`: Data storage directory (default: ~/.pkl)
- `CURSOR_DB_PATH`: Cursor database path (auto-detected)

### Customization
- **CSS Variables**: Modify `assets/css/variables.css` for theming
- **Dashboard Layout**: Update `assets/css/main.css` for styling
- **Functionality**: Extend `assets/js/dashboard.js` for features

## Development

### Building
```bash
# Compile TypeScript
npm run build

# Watch for changes
npm run dev

# Run tests
npm test
```


### Common Issues

#### No Sessions Detected
- Ensure Cursor IDE is running
- Check that a `.ipynb` file is open
- Verify file monitoring is active

#### Conversation Capture Not Working
- Check Cursor database accessibility
- Ensure proper permissions
- Review server logs for errors

#### Dashboard Not Loading
- Verify server is running on correct port
- Check browser console for errors
- Ensure all assets are properly served

### Debug Mode
Enable debug logging by setting `DEBUG=true` in environment variables.

## License

MIT License - see LICENSE file for details.

## Support

For issues and questions:
- Create an issue in the repository
- Check the troubleshooting section
- Review the API documentation

## Changelog

### Version 1.0.0
- Initial release
- Real-time session tracking
- Conversation capture
- Dashboard interface
- Export functionality
- macOS integration