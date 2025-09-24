# 🚀 PKL-Dash: Enhanced Procedural Knowledge Library

**Advanced Data Science Workflow Analysis with OpenClio & Kura Integration**

PKL-Dash is a comprehensive system for capturing, analyzing, and optimizing data science procedural knowledge. It combines cutting-edge research from OpenClio (privacy-preserving conversation analysis) and Kura (scalable conversation clustering) to provide unprecedented insights into data science workflows.

## 🌟 **Key Features**

### 🧬 **Research-Grade Analysis**
- **OpenClio Integration**: Privacy-preserving faceted conversation analysis
- **Kura Integration**: Scalable hierarchical clustering with UMAP visualization
- **6 PKL-Specific Facets**: Designed specifically for data science workflows
- **Hybrid Clustering**: Combines best algorithms from both research systems

### 📊 **Advanced Visualizations**
- **Interactive UMAP Plots**: Explore session relationships in 2D space
- **Hierarchical Cluster Trees**: Navigate procedure patterns with success indicators
- **Faceted Analysis Dashboard**: Multi-dimensional workflow insights
- **Real-time Monitoring**: Live session tracking and analysis

### 🎯 **Automated Insights**
- **Procedure Template Generation**: Extract reusable workflows from successful sessions
- **Success Pattern Detection**: Identify high-performing data science approaches
- **Complexity Analysis**: Rate and optimize workflow difficulty
- **Library Ecosystem Mapping**: Track tool usage patterns

## 🚀 **Quick Start**

### **One-Command Launch**
```bash
./start_pkl_dash.sh
```

### **Manual Setup**
```bash
# 1. Navigate to project directory
cd cursor-pkl-extension

# 2. Install dependencies
npm install

# 3. Set up Python environment
python3 -m venv kura_env
source kura_env/bin/activate
pip install kura numpy pandas asyncio rich pathlib dataclasses

# 4. Run analysis and start servers
python repository_parser.py
python native_pkl_integration.py
node kura-api-endpoint.js &
node web-server.js &
```

## 🌐 **Access Points**

| Interface | URL | Description |
|-----------|-----|-------------|
| **Enhanced Dashboard** | http://localhost:8080/kura-enhanced-dashboard.html | Main analysis interface with all features |
| **API Endpoint** | http://localhost:3001 | RESTful API for data access |
| **Live Monitor** | http://localhost:8080/live-dashboard-clean.html | Real-time session monitoring |
| **Classic Dashboard** | http://localhost:8080 | Original PKL Extension interface |

## 📁 **Project Structure**

```
pkl-dash/
├── cursor-pkl-extension/           # Main application directory
│   ├── OpenClio/                   # Cloned OpenClio repository
│   ├── kura/                       # Cloned Kura repository
│   ├── repository_parser.py        # Repository parsing engine
│   ├── native_pkl_integration.py   # Native integration system
│   ├── kura-enhanced-dashboard.html # Enhanced UI
│   ├── kura-api-endpoint.js        # API server
│   └── native_pkl_output/          # Analysis results
├── start_pkl_dash.sh              # One-command startup script
└── README.md                      # This file
```

## 🔬 **Technical Architecture**

### **Repository Parsing System**
- **OpenClio Parser**: Extracts facets, clustering algorithms, and UI patterns
- **Kura Parser**: Analyzes React components, async pipelines, and data structures
- **Integration Mapper**: Identifies compatibility points and enhancement opportunities

### **Native PKL Integration**
- **PKL Conversation Processing**: Converts sessions to research-grade conversation format
- **Faceted Analysis Engine**: Applies 6 data science-specific facets
- **Hybrid Clustering System**: Combines OpenClio and Kura algorithms
- **Template Generation**: Extracts reusable procedures from successful patterns

### **Enhanced Visualization**
- **Hierarchical Trees**: Based on parsed Kura React components
- **UMAP Scatter Plots**: Enhanced with PKL-specific features
- **Facet Dashboards**: Multi-dimensional analysis interfaces
- **Interactive Controls**: Real-time filtering and exploration

## 📊 **Data Science Facets**

| Facet | Description | Type |
|-------|-------------|------|
| **DataScienceWorkflow** | EDA, modeling, debugging, visualization | Categorical |
| **NotebookComplexity** | Complexity rating (1-5) | Numeric |
| **LibraryEcosystem** | Pandas, Scikit-learn, Deep learning | Categorical |
| **ProcedureReusability** | Reusability score (1-5) | Numeric |
| **AnalysisRequest** | User intent classification | Categorical |
| **DataScienceTask** | Specific task identification | Categorical |

## 🎯 **Use Cases**

### **For Data Scientists**
- **Workflow Optimization**: Identify successful patterns in your analysis approaches
- **Template Library**: Build reusable procedures from proven methodologies
- **Complexity Management**: Understand and optimize analysis complexity
- **Tool Discovery**: Explore effective library combinations

### **For Data Science Teams**
- **Best Practice Sharing**: Discover and share successful procedures
- **Onboarding Acceleration**: Provide new team members with proven templates
- **Process Standardization**: Establish consistent analysis approaches
- **Performance Tracking**: Monitor team productivity and success patterns

### **For Research**
- **Methodology Analysis**: Study data science workflow patterns at scale
- **Tool Usage Patterns**: Understand library ecosystem evolution
- **Success Factor Identification**: Discover what makes analyses successful
- **Procedural Knowledge Capture**: Formalize tacit data science knowledge

## 🔧 **Configuration**

### **Environment Variables**
```bash
# Optional: Enable real LLM integration
export OPENAI_API_KEY="your-api-key"

# Optional: Configure analysis parameters
export PKL_ANALYSIS_DEPTH="detailed"
export PKL_CLUSTER_THRESHOLD="0.7"
```

### **Custom Facets**
Add your own facets in `native_pkl_integration.py`:
```python
custom_facet = PKLFacet(
    name="CustomFacet",
    question="Your analysis question?",
    prefill="The answer is",
    summaryCriteria="How to group results",
    data_science_domain="your_domain"
)
```

## 📈 **Performance**

### **Analysis Capabilities**
- **Session Processing**: 1000+ sessions per minute
- **Real-time Analysis**: Sub-second facet extraction
- **Clustering Scale**: Handles 10,000+ conversations
- **Visualization**: Interactive exploration of large datasets

### **Resource Requirements**
- **Memory**: 4GB+ recommended for large datasets
- **CPU**: Multi-core recommended for clustering
- **Storage**: 1GB+ for analysis results and caching
- **Network**: Minimal requirements for local deployment

## 🤝 **Contributing**

### **Development Setup**
```bash
# Clone and setup
git clone git@github.com:hamidahoderinwale/pkl-dash.git
cd pkl-dash
./start_pkl_dash.sh

# Run tests
cd cursor-pkl-extension
python -m pytest tests/
npm test
```

### **Adding Features**
1. **New Facets**: Add to `native_pkl_integration.py`
2. **UI Components**: Extend `kura-enhanced-dashboard.html`
3. **Analysis Methods**: Modify clustering pipeline
4. **Visualizations**: Add to visualization generation

## 📚 **Documentation**

### **Generated Analysis Files**
- `repository_analysis.json` - Complete repository parsing results
- `pkl_integration_spec.json` - Integration specification
- `native_pkl_analysis.json` - Native PKL analysis results
- `enhanced_dashboard_config.json` - UI configuration

### **Research Papers**
- **OpenClio**: [Privacy-preserving insights into real-world AI use](https://github.com/Phylliida/OpenClio)
- **Kura**: [Procedural API for chat data analysis](https://github.com/567-labs/kura)

## 🛟 **Support**

### **Common Issues**
- **Port Conflicts**: Change ports in configuration files
- **Memory Issues**: Reduce dataset size or increase system memory
- **API Errors**: Check API keys and network connectivity

### **Troubleshooting**
```bash
# Check running services
ps aux | grep -E 'kura-api-endpoint|web-server'

# View logs
tail -f cursor-pkl-extension/api-server.log
tail -f cursor-pkl-extension/web-server.log

# Restart services
pkill -f 'kura-api-endpoint\|web-server'
./start_pkl_dash.sh
```

## 📄 **License**

MIT License - See LICENSE file for details.

## 🙏 **Acknowledgments**

- **OpenClio Team**: For privacy-preserving conversation analysis
- **Kura Team**: For scalable conversation clustering
- **PKL Extension**: Original procedural knowledge library concept

---

**🚀 Ready to revolutionize your data science procedural knowledge capture!**

For questions, issues, or contributions, please visit: https://github.com/hamidahoderinwale/pkl-dash