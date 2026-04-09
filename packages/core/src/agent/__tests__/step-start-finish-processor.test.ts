import { convertArrayToReadableStream, MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { describe, expect, it } from 'vitest';
import { Agent } from '../agent';

describe('Step Start/Finish in processOutputStream', () => {
  it('should receive step-start and step-finish in processOutputStream', async () => {
    const capturedParts: string[] = [];

    const processor: any = {
      id: 'step-tracker',
      name: 'Step Tracker',
      processOutputStream: async ({ part }: any) => {
        capturedParts.push(part.type);
        return part;
      },
    };

    const mockModel = new MockLanguageModelV2({
      doStream: async () => {
        return {
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
            { type: 'text-start', id: 'text-1' },
            { type: 'text-delta', id: 'text-1', delta: 'Hello world' },
            { type: 'text-end', id: 'text-1' },
            {
              type: 'finish',
              finishReason: 'stop',
              usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 },
            },
          ]),
          rawCall: { rawPrompt: [], rawSettings: {} },
          warnings: [],
        };
      },
    });

    const agent = new Agent({
      id: 'test-agent',
      name: 'Test Agent',
      instructions: 'You are a helpful assistant.',
      model: mockModel as any,
      outputProcessors: [processor],
    });

    const stream = await agent.stream('Say hello');

    const chunkTypes: string[] = [];
    for await (const chunk of stream.fullStream) {
      chunkTypes.push(chunk.type);
    }

    console.log('Chunk types in fullStream:', chunkTypes);
    console.log('Parts captured in processOutputStream:', capturedParts);

    // Verify step-start and step-finish are in the fullStream
    expect(chunkTypes).toContain('step-start');
    expect(chunkTypes).toContain('step-finish');

    // Verify step-start and step-finish were passed to processOutputStream
    expect(capturedParts).toContain('step-start');
    expect(capturedParts).toContain('step-finish');
  });
});
