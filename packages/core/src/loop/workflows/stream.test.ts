import { describe, expect, it, vi } from 'vitest';
import { MessageList } from '../../agent/message-list';
import type { ProcessorStreamWriter } from '../../processors';
import { ChunkFrom } from '../../stream/types';
import type { ChunkType } from '../../stream/types';

// Capture the outputWriter passed to createAgenticLoopWorkflow so we can
// invoke it directly in tests without spinning up a real agentic loop.
let capturedOutputWriter: ((chunk: ChunkType) => Promise<void>) | undefined;

vi.mock('./agentic-loop', () => ({
  createAgenticLoopWorkflow: (params: any) => {
    capturedOutputWriter = params.outputWriter;

    return {
      __registerMastra: vi.fn(),
      deleteWorkflowRunById: vi.fn().mockResolvedValue(undefined),
      createRun: vi.fn().mockResolvedValue({
        start: vi.fn().mockImplementation(async () => {
          // Simulate the agentic loop emitting a data-* chunk AND step-start/step-finish
          await capturedOutputWriter!({
            type: 'data-moderation',
            data: { flagged: true },
            runId: 'run-1',
            from: ChunkFrom.AGENT,
          } as unknown as ChunkType);

          // Emit step-start
          await capturedOutputWriter!({
            type: 'step-start',
            runId: 'run-1',
            from: ChunkFrom.AGENT,
            payload: { request: {}, warnings: [], messageId: 'msg-1' },
          } as unknown as ChunkType);

          // Emit step-finish
          await capturedOutputWriter!({
            type: 'step-finish',
            runId: 'run-1',
            from: ChunkFrom.AGENT,
            payload: {
              output: { steps: [], usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 } },
              stepResult: {
                reason: 'stop' as const,
                warnings: [],
                isContinued: false,
                totalUsage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
              },
              metadata: {},
              messages: { nonUser: [], all: [], user: [] },
            },
          } as unknown as ChunkType);

          return {
            status: 'success',
            result: {
              output: { steps: [], usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 } },
              stepResult: { reason: 'stop', warnings: [], isContinued: false },
              metadata: {},
              messages: { nonUser: [], all: [] },
            },
          };
        }),
      }),
    };
  },
}));

const { workflowLoopStream } = await import('./stream');

describe('workflowLoopStream', () => {
  it('should pass a defined writer to output processors when processing data-* chunks', async () => {
    let receivedWriter: ProcessorStreamWriter | undefined;

    const processor: any = {
      id: 'writer-capture',
      name: 'Writer Capture',
      processOutputStream: async ({ part, writer }: any) => {
        receivedWriter = writer;
        return part;
      },
    };

    const messageList = new MessageList({ threadId: 'test-thread' });

    const stream = workflowLoopStream({
      messageId: 'msg-1',
      runId: 'run-1',
      startTimestamp: Date.now(),
      agentId: 'test-agent',
      messageList,
      models: [{ model: {} as any, maxRetries: 3, id: 'test-model' }],
      outputProcessors: [processor],
      _internal: {},
      streamState: { serialize: () => ({}), deserialize: () => {} },
      methodType: 'stream',
    });

    // Consume the stream
    const reader = stream.getReader();
    const chunks: ChunkType[] = [];
    while (true) {
      const { done, value } = await reader.read();
      if (value) chunks.push(value);
      if (done) break;
    }

    // The processor should have received a defined writer
    expect(receivedWriter).toBeDefined();
    expect(typeof receivedWriter!.custom).toBe('function');

    // Verify the data-* chunk was emitted
    const dataChunk = chunks.find(c => c.type === 'data-moderation');
    expect(dataChunk).toBeDefined();
  });

  it('should pass step-start chunks through output processors', async () => {
    const capturedChunkTypes: string[] = [];

    const processor: any = {
      id: 'step-tracking',
      name: 'Step Tracking',
      processOutputStream: async ({ part }: any) => {
        capturedChunkTypes.push(part.type);
        return part;
      },
    };

    const messageList = new MessageList({ threadId: 'test-thread' });

    const stream = workflowLoopStream({
      messageId: 'msg-1',
      runId: 'run-1',
      startTimestamp: Date.now(),
      agentId: 'test-agent',
      messageList,
      models: [{ model: {} as any, maxRetries: 3, id: 'test-model' }],
      outputProcessors: [processor],
      _internal: {},
      streamState: { serialize: () => ({}), deserialize: () => {} },
      methodType: 'stream',
    });

    // Consume the stream
    const reader = stream.getReader();
    while (true) {
      const { done } = await reader.read();
      if (done) break;
    }

    // The processor should have received step-start
    expect(capturedChunkTypes).toContain('step-start');
  });

  it('should pass step-finish chunks through output processors', async () => {
    const capturedChunkTypes: string[] = [];

    const processor: any = {
      id: 'step-tracking',
      name: 'Step Tracking',
      processOutputStream: async ({ part }: any) => {
        capturedChunkTypes.push(part.type);
        return part;
      },
    };

    const messageList = new MessageList({ threadId: 'test-thread' });

    const stream = workflowLoopStream({
      messageId: 'msg-1',
      runId: 'run-1',
      startTimestamp: Date.now(),
      agentId: 'test-agent',
      messageList,
      models: [{ model: {} as any, maxRetries: 3, id: 'test-model' }],
      outputProcessors: [processor],
      _internal: {},
      streamState: { serialize: () => ({}), deserialize: () => {} },
      methodType: 'stream',
    });

    // Consume the stream
    const reader = stream.getReader();
    while (true) {
      const { done } = await reader.read();
      if (done) break;
    }

    // The processor should have received step-finish
    expect(capturedChunkTypes).toContain('step-finish');
  });
});
