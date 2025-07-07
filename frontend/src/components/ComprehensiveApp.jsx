import { useState } from 'react'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs.jsx'
import { Bot, Brain, GitBranch, Shield, Workflow, BarChart3, Zap } from 'lucide-react'

// Import individual feature components
import TextProcessor from './TextProcessor.jsx'
import ActiveLearning from './ActiveLearning.jsx'
import DataVersioning from './DataVersioning.jsx'
import AdvancedValidation from './AdvancedValidation.jsx'
import IntegrationHub from './IntegrationHub.jsx'
import WorkflowAutomation from './WorkflowAutomation.jsx'
import AnalyticsDashboard from './AnalyticsDashboard.jsx'

const ComprehensiveApp = () => {
  const [activeTab, setActiveTab] = useState('labeling')

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="bg-white shadow-sm border-b">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="py-4">
            <h1 className="text-2xl font-bold text-gray-900">
              Multi-Agent Data Labeling System
            </h1>
            <p className="text-gray-600 mt-1">
              Advanced AI-powered data labeling with active learning and automation
            </p>
          </div>
        </div>
      </div>

      <div className="max-w-7xl mx-auto p-6">
        <Tabs value={activeTab} onValueChange={setActiveTab} className="w-full">
          <TabsList className="grid w-full grid-cols-7 mb-6">
            <TabsTrigger value="labeling" className="flex items-center gap-2">
              <Bot className="w-4 h-4" />
              Labeling
            </TabsTrigger>
            <TabsTrigger value="active-learning" className="flex items-center gap-2">
              <Brain className="w-4 h-4" />
              Active Learning
            </TabsTrigger>
            <TabsTrigger value="versioning" className="flex items-center gap-2">
              <GitBranch className="w-4 h-4" />
              Versioning
            </TabsTrigger>
            <TabsTrigger value="validation" className="flex items-center gap-2">
              <Shield className="w-4 h-4" />
              Validation
            </TabsTrigger>
            <TabsTrigger value="integration" className="flex items-center gap-2">
              <Zap className="w-4 h-4" />
              Integration
            </TabsTrigger>
            <TabsTrigger value="workflows" className="flex items-center gap-2">
              <Workflow className="w-4 h-4" />
              Workflows
            </TabsTrigger>
            <TabsTrigger value="analytics" className="flex items-center gap-2">
              <BarChart3 className="w-4 h-4" />
              Analytics
            </TabsTrigger>
          </TabsList>

          <TabsContent value="labeling" className="mt-0">
            <TextProcessor />
          </TabsContent>

          <TabsContent value="active-learning" className="mt-0">
            <ActiveLearning />
          </TabsContent>

          <TabsContent value="versioning" className="mt-0">
            <DataVersioning />
          </TabsContent>

          <TabsContent value="validation" className="mt-0">
            <AdvancedValidation />
          </TabsContent>

          <TabsContent value="integration" className="mt-0">
            <IntegrationHub />
          </TabsContent>

          <TabsContent value="workflows" className="mt-0">
            <WorkflowAutomation />
          </TabsContent>

          <TabsContent value="analytics" className="mt-0">
            <AnalyticsDashboard />
          </TabsContent>
        </Tabs>
      </div>
    </div>
  )
}

export default ComprehensiveApp
