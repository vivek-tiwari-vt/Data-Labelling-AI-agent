import React from 'react'

function TestApp() {
  return (
    <div style={{ padding: '20px', fontSize: '18px', color: 'black', backgroundColor: 'white' }}>
      <h1>🎉 Frontend is Working!</h1>
      <p>This is a test to see if React is rendering properly.</p>
      <div style={{ marginTop: '20px' }}>
        <h2>Test Features:</h2>
        <ul>
          <li>✅ React is rendering</li>
          <li>✅ JavaScript is working</li>
          <li>✅ Styles are applying</li>
        </ul>
      </div>
    </div>
  )
}

export default TestApp
