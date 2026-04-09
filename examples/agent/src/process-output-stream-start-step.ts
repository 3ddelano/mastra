import { Agent } from '@mastra/core/agent';
import { Processor, ProcessOutputStreamArgs } from '@mastra/core/processors';
import { createTool } from '@mastra/core/tools';
import { ChunkType } from '@mastra/core/workflows';
import z from 'zod';

class StepStartFinishEmitter implements Processor {
  readonly id = 'StepStartFinishEmitter';
  readonly name = 'StepStartFinishEmitter';

  async processOutputStream({ part }: ProcessOutputStreamArgs<unknown>): Promise<ChunkType | null | undefined> {
    if (part.type == 'step-start') {
      console.log('got step start in stream');
    }
    if (part.type == 'step-finish') {
      console.log('got step finish in stream with usage', (part as any).payload?.output?.usage);
    }
    return part;
  }
}

async function main() {
  console.log('running main');

  const weatherTool = createTool({
    id: 'weather',
    description: 'Get the weather for a city',
    inputSchema: z.object({
      city: z.string(),
    }),
    execute: async () => {
      const randomm = Math.floor(Math.random() * 100);
      return 'The weather is ' + randomm;
    },
  });

  const simpleAgent = new Agent({
    id: 'simple-agent',
    name: 'Simple Agent',
    instructions: `You are a coding assistant.`,
    model: 'vercel/google/gemini-2.0-flash',
    outputProcessors: [new StepStartFinishEmitter()],
    tools: { weather: weatherTool },
  });

  const stream = await simpleAgent.stream({
    id: 'msg-123',
    role: 'user',
    parts: [{ type: 'text', text: 'whats the weather in nyc and washington?' }],
  });

  for await (const chunk of stream.fullStream) {
    // console.log('\ngot chunk from fullStream:', JSON.stringify(chunk.type));
  }
}

main();
