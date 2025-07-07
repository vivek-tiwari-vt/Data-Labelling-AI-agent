import React from 'react';

class ErrorBoundary extends React.Component {
  constructor(props) {
    super(props);
    this.state = { hasError: false, error: null, errorInfo: null };
  }

  static getDerivedStateFromError(error) {
    // Update state so the next render will show the fallback UI
    return { hasError: true };
  }

  componentDidCatch(error, errorInfo) {
    // Log error details for debugging
    console.error('ErrorBoundary caught an error:', error);
    console.error('Component stack:', errorInfo.componentStack);
    
    // Update state with error details
    this.setState({
      error: error,
      errorInfo: errorInfo
    });
  }

  render() {
    if (this.state.hasError) {
      // Custom fallback UI with detailed error information
      return (
        <div style={{ 
          padding: '20px', 
          border: '2px solid red', 
          borderRadius: '8px', 
          backgroundColor: '#ffe6e6',
          margin: '20px',
          fontFamily: 'monospace'
        }}>
          <h2 style={{ color: 'red', marginTop: 0 }}>⚠️ Something went wrong!</h2>
          <details style={{ whiteSpace: 'pre-wrap', marginBottom: '10px' }}>
            <summary style={{ cursor: 'pointer', fontWeight: 'bold' }}>
              Error Details (click to expand)
            </summary>
            <div style={{ marginTop: '10px', padding: '10px', backgroundColor: '#f0f0f0' }}>
              <h4>Error:</h4>
              <pre style={{ color: 'red' }}>{this.state.error && this.state.error.toString()}</pre>
              
              <h4>Component Stack:</h4>
              <pre style={{ color: 'blue' }}>
                {this.state.errorInfo && this.state.errorInfo.componentStack}
              </pre>
              
              <h4>Error Stack:</h4>
              <pre style={{ color: 'gray', fontSize: '12px' }}>
                {this.state.error && this.state.error.stack}
              </pre>
            </div>
          </details>
          
          <button 
            onClick={() => this.setState({ hasError: false, error: null, errorInfo: null })}
            style={{
              padding: '10px 20px',
              backgroundColor: '#4CAF50',
              color: 'white',
              border: 'none',
              borderRadius: '4px',
              cursor: 'pointer'
            }}
          >
            Try Again
          </button>
          
          <div style={{ marginTop: '15px', fontSize: '14px', color: '#666' }}>
            <p><strong>Troubleshooting tips:</strong></p>
            <ul>
              <li>Check the browser console for additional errors</li>
              <li>Verify all imports are correct</li>
              <li>Check that all required dependencies are installed</li>
              <li>Look for typos in component names or props</li>
            </ul>
          </div>
        </div>
      );
    }

    return this.props.children;
  }
}

export default ErrorBoundary;
