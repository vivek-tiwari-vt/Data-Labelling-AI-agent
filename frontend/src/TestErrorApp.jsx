import React from 'react'
import ErrorBoundary from './components/ErrorBoundary.jsx'
import './App.css'

// Test component that intentionally throws an error
function TestErrorComponent() {
  const throwError = () => {
    throw new Error('Test error to verify error boundary is working!')
  }
  
  return (
    <div>
      <h1>Error Boundary Test</h1>
      <button onClick={throwError}>Click to trigger error</button>
    </div>
  )
}

// Simple test component that should always work
function SimpleComponent() {
  return (
    <div style={{ padding: '20px' }}>
      <h1>Simple Test Component</h1>
      <p>If you can see this, React is working correctly!</p>
      <button>Test Button</button>
    </div>
  )
}

function App() {
  return (
    <div className="min-h-screen bg-gray-50">
      <ErrorBoundary>
        <SimpleComponent />
      </ErrorBoundary>
    </div>
  )
}

export default App
