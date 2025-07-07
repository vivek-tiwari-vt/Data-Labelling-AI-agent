import React from 'react'
import { Button } from '@/components/ui/button.jsx'
import './App.css'

function MinimalUITest() {
  return (
    <div style={{ padding: '20px' }}>
      <h1>UI Component Test</h1>
      <p>Testing if UI components work...</p>
      <Button>Test Button</Button>
      <div style={{ marginTop: '10px' }}>
        <Button variant="secondary">Secondary Button</Button>
      </div>
    </div>
  )
}

export default MinimalUITest
