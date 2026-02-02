import { EnvironmentEnum } from '../general';
import pino, { Logger } from 'pino';
import { getSSMParameter } from '../ssm';
import { SSMClient, GetParameterCommand } from "@aws-sdk/client-ssm";
import { mockClient } from "aws-sdk-client-mock";

const ssmMock = mockClient(SSMClient);
const mockLogger: Logger = pino({ level: 'silent' });

describe('getSSMParameter', () => {
  beforeEach(() => {
    ssmMock.reset();
  });

  it('returns the parameter value on success', async () => {
    ssmMock.on(GetParameterCommand).resolves({
      Parameter: { Value: 'Testing' }
    });

    const result = await getSSMParameter(
      {
        logger: mockLogger,
        region: 'us-west-2',
        useTLS: false
      },
      'my-key',
      EnvironmentEnum.DEV
    );

    expect(result).toEqual('Testing');
  });

  it('returns undefined and logs error on failure', async () => {
    ssmMock.on(GetParameterCommand).rejects(new Error('AWS Error'));

    const result = await getSSMParameter(
      {
        logger: mockLogger,
        region: 'us-west-2',
        useTLS: false
      },
      'my-key',
      EnvironmentEnum.DEV
    );

    expect(result).toBeUndefined();
  });

  it('returns undefined if no key is specified', async () => {
    const result = await getSSMParameter(
      {
        logger: mockLogger,
        region: 'us-west-2',
        useTLS: false
      },
      '',
      EnvironmentEnum.DEV
    );
    expect(result).toBeUndefined();
  });
});
