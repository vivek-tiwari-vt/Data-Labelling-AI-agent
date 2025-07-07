import React from 'react'
import { Button } from '@/components/ui/button.jsx'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card.jsx'

// Test if UI components work
function DebugApp2() {
  return (
    <div className="min-h-screen bg-gray-50 p-4">
      <h1 className="text-3xl font-bold text-center mb-4">Debug App - Step 2</h1>
      <p className="text-center mb-6">Testing UI Components...</p>
      
      <div className="max-w-md mx-auto">
        <Card>
          <CardHeader>
            <CardTitle>Test Card</CardTitle>
            <CardDescription>This tests if shadcn/ui components work</CardDescription>
          </CardHeader>
          <CardContent>
            <Button onClick={() => alert('Button works!')}>Test Button</Button>
          </CardContent>
        </Card>
      </div>
    </div>
  )
}

export default DebugApp2
