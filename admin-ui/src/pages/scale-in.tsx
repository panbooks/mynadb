import { useState } from 'react'
import {
  ArrowDownToLine,
  ChevronRight,
  ChevronLeft,
  AlertTriangle,
  CheckCircle2,
  Play,
  Loader2,
  Server,
  XCircle,
} from 'lucide-react'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Badge } from '@/components/ui/badge'
import { Progress } from '@/components/ui/progress'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Separator } from '@/components/ui/separator'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'
import {
  useScaleInRecommendations,
  useCreateScaleInPlan,
  useExecuteScaleIn,
  useScaleInStatus,
  useCancelScaleIn,
} from '@/hooks/use-scale-in'
import { useNodes } from '@/hooks/use-nodes'
import { formatDuration } from '@/lib/utils'
import type { ScaleInStrategy, ScaleInPlan, ScaleInRecommendation } from '@/lib/types'

type WizardStep = 'configure' | 'preview' | 'execute'

export function ScaleInPage() {
  const [step, setStep] = useState<WizardStep>('configure')
  const [nodeCount, setNodeCount] = useState(1)
  const [strategy, setStrategy] = useState<ScaleInStrategy>('LeastLoaded')
  const [selectedNodeIds, setSelectedNodeIds] = useState<string[]>([])
  const [plan, setPlan] = useState<ScaleInPlan | null>(null)
  const [executingPlanId, setExecutingPlanId] = useState<string | null>(null)

  const { data: recommendations } = useScaleInRecommendations(nodeCount, step === 'configure')
  const { data: dataNodes } = useNodes({ role: 'Data', state: 'Active' })
  const createPlan = useCreateScaleInPlan()
  const executeScaleIn = useExecuteScaleIn()
  const cancelScaleIn = useCancelScaleIn()
  const { data: executionStatus } = useScaleInStatus(
    executingPlanId,
    step === 'execute' && !!executingPlanId
  )

  const handleGeneratePlan = async () => {
    const result = await createPlan.mutateAsync({
      node_count: nodeCount,
      strategy,
      specific_node_ids: selectedNodeIds.length > 0 ? selectedNodeIds : undefined,
      dry_run: true,
    })
    setPlan(result)
    setStep('preview')
  }

  const handleExecute = async () => {
    if (!plan) return

    // Create the actual plan (not dry run)
    const actualPlan = await createPlan.mutateAsync({
      node_count: nodeCount,
      strategy,
      specific_node_ids: selectedNodeIds.length > 0 ? selectedNodeIds : undefined,
      dry_run: false,
    })

    setExecutingPlanId(actualPlan.id)
    setStep('execute')

    await executeScaleIn.mutateAsync(actualPlan.id)
  }

  const handleCancel = async () => {
    if (executingPlanId) {
      await cancelScaleIn.mutateAsync(executingPlanId)
    }
    resetWizard()
  }

  const resetWizard = () => {
    setStep('configure')
    setNodeCount(1)
    setStrategy('LeastLoaded')
    setSelectedNodeIds([])
    setPlan(null)
    setExecutingPlanId(null)
  }

  const isComplete = executionStatus?.status === 'Completed'
  const isFailed = executionStatus?.status === 'Failed'

  return (
    <div className="space-y-6">
      {/* Header */}
      <div>
        <h1 className="text-3xl font-bold">Scale-In Operations</h1>
        <p className="text-muted-foreground">
          Safely reduce cluster size by retiring data nodes
        </p>
      </div>

      {/* Progress Steps */}
      <div className="flex items-center justify-center gap-4">
        {(['configure', 'preview', 'execute'] as WizardStep[]).map((s, i) => (
          <div key={s} className="flex items-center gap-2">
            <div
              className={`flex h-8 w-8 items-center justify-center rounded-full ${
                step === s
                  ? 'bg-primary text-primary-foreground'
                  : i < ['configure', 'preview', 'execute'].indexOf(step)
                  ? 'bg-green-500 text-white'
                  : 'bg-muted text-muted-foreground'
              }`}
            >
              {i < ['configure', 'preview', 'execute'].indexOf(step) ? (
                <CheckCircle2 className="h-4 w-4" />
              ) : (
                i + 1
              )}
            </div>
            <span
              className={`text-sm font-medium capitalize ${
                step === s ? 'text-foreground' : 'text-muted-foreground'
              }`}
            >
              {s}
            </span>
            {i < 2 && <ChevronRight className="h-4 w-4 text-muted-foreground" />}
          </div>
        ))}
      </div>

      {/* Step Content */}
      {step === 'configure' && (
        <ConfigureStep
          nodeCount={nodeCount}
          setNodeCount={setNodeCount}
          strategy={strategy}
          setStrategy={setStrategy}
          selectedNodeIds={selectedNodeIds}
          setSelectedNodeIds={setSelectedNodeIds}
          recommendations={recommendations}
          dataNodes={dataNodes}
          onNext={handleGeneratePlan}
          isLoading={createPlan.isPending}
        />
      )}

      {step === 'preview' && plan && (
        <PreviewStep
          plan={plan}
          onBack={() => setStep('configure')}
          onExecute={handleExecute}
          isLoading={executeScaleIn.isPending || createPlan.isPending}
        />
      )}

      {step === 'execute' && (
        <ExecuteStep
          status={executionStatus}
          isComplete={isComplete}
          isFailed={isFailed}
          onCancel={handleCancel}
          onReset={resetWizard}
        />
      )}
    </div>
  )
}

interface ConfigureStepProps {
  nodeCount: number
  setNodeCount: (n: number) => void
  strategy: ScaleInStrategy
  setStrategy: (s: ScaleInStrategy) => void
  selectedNodeIds: string[]
  setSelectedNodeIds: (ids: string[]) => void
  recommendations?: ScaleInRecommendation[]
  dataNodes?: { id: string; mode?: string; tables_owned?: number }[]
  onNext: () => void
  isLoading: boolean
}

function ConfigureStep({
  nodeCount,
  setNodeCount,
  strategy,
  setStrategy,
  selectedNodeIds,
  setSelectedNodeIds,
  recommendations,
  dataNodes,
  onNext,
  isLoading,
}: ConfigureStepProps) {
  const toggleNodeSelection = (nodeId: string) => {
    setSelectedNodeIds(
      selectedNodeIds.includes(nodeId)
        ? selectedNodeIds.filter((id) => id !== nodeId)
        : [...selectedNodeIds, nodeId]
    )
  }

  return (
    <div className="space-y-6">
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Configuration</CardTitle>
          <CardDescription>
            Select how many nodes to retire and the selection strategy
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-6">
          <div className="grid gap-6 md:grid-cols-2">
            <div>
              <Label htmlFor="node-count">Number of Nodes to Retire</Label>
              <Input
                id="node-count"
                type="number"
                min={1}
                max={dataNodes?.length ?? 10}
                value={nodeCount}
                onChange={(e) => setNodeCount(parseInt(e.target.value) || 1)}
                className="mt-1"
              />
            </div>

            <div>
              <Label htmlFor="strategy">Selection Strategy</Label>
              <Select
                value={strategy}
                onValueChange={(v) => setStrategy(v as ScaleInStrategy)}
              >
                <SelectTrigger id="strategy" className="mt-1">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="LeastLoaded">
                    <div>
                      <p className="font-medium">Least Loaded</p>
                      <p className="text-xs text-muted-foreground">
                        Retire nodes with fewest tables
                      </p>
                    </div>
                  </SelectItem>
                  <SelectItem value="FirstIn">
                    <div>
                      <p className="font-medium">First In</p>
                      <p className="text-xs text-muted-foreground">
                        Retire oldest nodes first
                      </p>
                    </div>
                  </SelectItem>
                  <SelectItem value="LastIn">
                    <div>
                      <p className="font-medium">Last In</p>
                      <p className="text-xs text-muted-foreground">
                        Retire newest nodes first
                      </p>
                    </div>
                  </SelectItem>
                  <SelectItem value="HaUnit">
                    <div>
                      <p className="font-medium">HA Unit</p>
                      <p className="text-xs text-muted-foreground">
                        Retire primary/standby pairs together
                      </p>
                    </div>
                  </SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        </CardContent>
      </Card>

      {/* Recommendations */}
      {recommendations && recommendations.length > 0 && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Recommended Nodes</CardTitle>
            <CardDescription>
              Based on your configuration, these nodes are recommended for retirement
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-2">
              {recommendations.map((rec) => (
                <div
                  key={rec.node_id}
                  className="flex items-center justify-between p-3 rounded-lg border"
                >
                  <div className="flex items-center gap-3">
                    <Server className="h-4 w-4 text-muted-foreground" />
                    <div>
                      <p className="font-mono text-sm">{rec.node_id}</p>
                      <p className="text-xs text-muted-foreground">{rec.reason}</p>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    {rec.mode && (
                      <Badge variant="outline">{rec.mode}</Badge>
                    )}
                    <Badge variant="secondary">{rec.tables_owned} tables</Badge>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      {/* Manual Node Selection */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Manual Selection (Optional)</CardTitle>
          <CardDescription>
            Or select specific nodes to retire
          </CardDescription>
        </CardHeader>
        <CardContent>
          <div className="grid gap-2 md:grid-cols-2 lg:grid-cols-3">
            {dataNodes?.map((node) => (
              <div
                key={node.id}
                onClick={() => toggleNodeSelection(node.id)}
                className={`flex items-center justify-between p-3 rounded-lg border cursor-pointer transition-colors ${
                  selectedNodeIds.includes(node.id)
                    ? 'border-primary bg-primary/5'
                    : 'hover:bg-accent'
                }`}
              >
                <div className="flex items-center gap-2">
                  <div
                    className={`h-4 w-4 rounded border ${
                      selectedNodeIds.includes(node.id)
                        ? 'bg-primary border-primary'
                        : 'border-muted-foreground'
                    }`}
                  >
                    {selectedNodeIds.includes(node.id) && (
                      <CheckCircle2 className="h-4 w-4 text-primary-foreground" />
                    )}
                  </div>
                  <span className="font-mono text-xs truncate max-w-[150px]">
                    {node.id}
                  </span>
                </div>
                {node.mode && (
                  <Badge variant="outline" className="text-xs">
                    {node.mode}
                  </Badge>
                )}
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <div className="flex justify-end">
        <Button onClick={onNext} disabled={isLoading}>
          {isLoading ? (
            <>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Generating Plan...
            </>
          ) : (
            <>
              Generate Plan
              <ChevronRight className="ml-2 h-4 w-4" />
            </>
          )}
        </Button>
      </div>
    </div>
  )
}

interface PreviewStepProps {
  plan: ScaleInPlan
  onBack: () => void
  onExecute: () => void
  isLoading: boolean
}

function PreviewStep({ plan, onBack, onExecute, isLoading }: PreviewStepProps) {
  return (
    <div className="space-y-6">
      {/* Warnings */}
      {plan.warnings.length > 0 && (
        <Alert variant="warning">
          <AlertTriangle className="h-4 w-4" />
          <AlertTitle>Warnings</AlertTitle>
          <AlertDescription>
            <ul className="mt-2 list-disc list-inside space-y-1">
              {plan.warnings.map((warning, i) => (
                <li key={i}>{warning}</li>
              ))}
            </ul>
          </AlertDescription>
        </Alert>
      )}

      {/* Summary */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Plan Summary</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid gap-4 md:grid-cols-3">
            <div>
              <p className="text-sm text-muted-foreground">Nodes to Retire</p>
              <p className="text-2xl font-bold">{plan.nodes_to_retire.length}</p>
            </div>
            <div>
              <p className="text-sm text-muted-foreground">Tables to Migrate</p>
              <p className="text-2xl font-bold">{plan.tables_to_migrate}</p>
            </div>
            {plan.estimated_duration_ms && (
              <div>
                <p className="text-sm text-muted-foreground">Estimated Duration</p>
                <p className="text-2xl font-bold">
                  {formatDuration(plan.estimated_duration_ms)}
                </p>
              </div>
            )}
          </div>
        </CardContent>
      </Card>

      {/* Nodes to Retire */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Nodes to Retire</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="flex flex-wrap gap-2">
            {plan.nodes_to_retire.map((nodeId) => (
              <Badge key={nodeId} variant="outline" className="font-mono">
                {nodeId}
              </Badge>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Phases */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base">Execution Phases</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-4">
            {plan.phases.map((phase) => (
              <div key={phase.phase_number} className="border rounded-lg p-4">
                <div className="flex items-center gap-2 mb-2">
                  <div className="flex h-6 w-6 items-center justify-center rounded-full bg-muted text-sm font-medium">
                    {phase.phase_number}
                  </div>
                  <p className="font-medium">{phase.description}</p>
                </div>
                <div className="pl-8 space-y-1">
                  {phase.operations.map((op, i) => (
                    <p key={i} className="text-sm text-muted-foreground">
                      {op.type}
                      {op.node_id && ` - ${op.node_id}`}
                      {op.table_key && ` - ${op.table_key}`}
                    </p>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <Separator />

      <div className="flex justify-between">
        <Button variant="outline" onClick={onBack}>
          <ChevronLeft className="mr-2 h-4 w-4" />
          Back
        </Button>
        <Button onClick={onExecute} disabled={isLoading}>
          {isLoading ? (
            <>
              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
              Starting...
            </>
          ) : (
            <>
              <Play className="mr-2 h-4 w-4" />
              Execute Plan
            </>
          )}
        </Button>
      </div>
    </div>
  )
}

interface ExecuteStepProps {
  status?: ScaleInPlan
  isComplete: boolean
  isFailed: boolean
  onCancel: () => void
  onReset: () => void
}

function ExecuteStep({ status, isComplete, isFailed, onCancel, onReset }: ExecuteStepProps) {
  const completedPhases = status?.phases.filter((p) => p.status === 'Completed').length ?? 0
  const totalPhases = status?.phases.length ?? 1
  const progress = (completedPhases / totalPhases) * 100

  return (
    <div className="space-y-6">
      {/* Status */}
      {isComplete && (
        <Alert variant="success">
          <CheckCircle2 className="h-4 w-4" />
          <AlertTitle>Scale-In Complete</AlertTitle>
          <AlertDescription>
            All nodes have been successfully retired.
          </AlertDescription>
        </Alert>
      )}

      {isFailed && (
        <Alert variant="destructive">
          <XCircle className="h-4 w-4" />
          <AlertTitle>Scale-In Failed</AlertTitle>
          <AlertDescription>
            The operation encountered an error. Check the phase status below for details.
          </AlertDescription>
        </Alert>
      )}

      {/* Progress */}
      <Card>
        <CardHeader>
          <CardTitle className="text-base flex items-center gap-2">
            {!isComplete && !isFailed && (
              <Loader2 className="h-4 w-4 animate-spin" />
            )}
            Execution Progress
          </CardTitle>
        </CardHeader>
        <CardContent>
          <div className="space-y-2">
            <div className="flex items-center justify-between text-sm">
              <span>
                Phase {completedPhases} of {totalPhases}
              </span>
              <span>{Math.round(progress)}%</span>
            </div>
            <Progress value={progress} />
          </div>
        </CardContent>
      </Card>

      {/* Phase Status */}
      {status?.phases && (
        <Card>
          <CardHeader>
            <CardTitle className="text-base">Phase Status</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              {status.phases.map((phase) => (
                <div
                  key={phase.phase_number}
                  className={`border rounded-lg p-4 ${
                    phase.status === 'InProgress' ? 'border-primary' : ''
                  }`}
                >
                  <div className="flex items-center justify-between mb-2">
                    <div className="flex items-center gap-2">
                      {phase.status === 'Completed' ? (
                        <CheckCircle2 className="h-5 w-5 text-green-500" />
                      ) : phase.status === 'InProgress' ? (
                        <Loader2 className="h-5 w-5 text-primary animate-spin" />
                      ) : phase.status === 'Failed' ? (
                        <XCircle className="h-5 w-5 text-destructive" />
                      ) : (
                        <div className="h-5 w-5 rounded-full border-2 border-muted" />
                      )}
                      <p className="font-medium">{phase.description}</p>
                    </div>
                    <Badge
                      variant={
                        phase.status === 'Completed'
                          ? 'success'
                          : phase.status === 'InProgress'
                          ? 'default'
                          : phase.status === 'Failed'
                          ? 'destructive'
                          : 'secondary'
                      }
                    >
                      {phase.status}
                    </Badge>
                  </div>

                  {phase.operations.length > 0 && (
                    <div className="pl-7 space-y-1">
                      {phase.operations.map((op, i) => (
                        <div key={i} className="flex items-center gap-2 text-sm">
                          {op.status === 'Completed' ? (
                            <CheckCircle2 className="h-3 w-3 text-green-500" />
                          ) : op.status === 'InProgress' ? (
                            <Loader2 className="h-3 w-3 animate-spin" />
                          ) : op.status === 'Failed' ? (
                            <XCircle className="h-3 w-3 text-destructive" />
                          ) : (
                            <div className="h-3 w-3 rounded-full border border-muted-foreground" />
                          )}
                          <span className="text-muted-foreground">
                            {op.type}
                            {op.node_id && ` - ${op.node_id}`}
                          </span>
                          {op.message && (
                            <span className="text-xs text-amber-500">({op.message})</span>
                          )}
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      )}

      <div className="flex justify-between">
        {!isComplete && !isFailed && (
          <Button variant="destructive" onClick={onCancel}>
            Cancel Operation
          </Button>
        )}
        {(isComplete || isFailed) && (
          <Button onClick={onReset}>
            <ArrowDownToLine className="mr-2 h-4 w-4" />
            New Scale-In
          </Button>
        )}
      </div>
    </div>
  )
}
