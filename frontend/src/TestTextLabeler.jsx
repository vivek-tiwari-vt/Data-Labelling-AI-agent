import React from 'react'

function TestTextLabeler() {
  return (
    <div className="max-w-4xl mx-auto p-6 space-y-6">
      <div className="text-center space-y-2">
        <h1 className="text-3xl font-bold">Multi-Agent Text Processing System</h1>
        <p className="text-gray-600">Frontend is working correctly!</p>
      </div>
      
      <div className="bg-white p-6 rounded-lg shadow">
        <h2 className="text-xl font-semibold mb-4">Status</h2>
        <ul className="space-y-2">
          <li className="flex items-center gap-2">
            <span className="w-2 h-2 bg-green-500 rounded-full"></span>
            React components loading
          </li>
          <li className="flex items-center gap-2">
            <span className="w-2 h-2 bg-green-500 rounded-full"></span>
            Tailwind CSS styling working
          </li>
          <li className="flex items-center gap-2">
            <span className="w-2 h-2 bg-green-500 rounded-full"></span>
            Component structure correct
          </li>
        </ul>
      </div>
    </div>
  )
}

export default TestTextLabeler
