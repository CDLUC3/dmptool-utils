import { queryTable } from '../rds';
import * as mysql from 'mysql2/promise';
import pino, { Logger } from 'pino';

jest.mock('mysql2/promise');

const mockLogger: Logger = pino({ level: 'silent' });
const mockConfig = {
  logger: mockLogger,
  host: 'localhost',
  port: 3306,
  user: 'test',
  password: 'test',
  database: 'testdb'
}

describe('queryTable', () => {
  let mockConnection: any;
  let mockQuery: jest.Mock;
  let mockEnd: jest.Mock;

  beforeEach(() => {
    mockQuery = jest.fn();
    mockEnd = jest.fn();
    mockConnection = {
      query: mockQuery,
      end: mockEnd,
    };
    (mysql.createConnection as jest.Mock).mockResolvedValue(mockConnection);
  });

  afterEach(() => {
    jest.clearAllMocks();
  });

  it('should execute a query and return results', async () => {
    const mockResults = [{id: 1, name: 'test'}];
    const mockFields = [{name: 'id'}, {name: 'name'}];
    mockQuery.mockResolvedValue([mockResults, mockFields]);

    const result = await queryTable(mockConfig, 'SELECT * FROM users');

    expect(result).toEqual({results: mockResults, fields: mockFields});
    expect(mockQuery).toHaveBeenCalledWith('SELECT * FROM users', []);
    expect(mockEnd).toHaveBeenCalled();
  });

  it('should execute a query with parameters', async () => {
    const mockResults = [{id: 1, name: 'test'}];
    const mockFields = [{name: 'id'}, {name: 'name'}];
    mockQuery.mockResolvedValue([mockResults, mockFields]);

    const result = await queryTable(mockConfig, 'SELECT * FROM users WHERE id = ?', [1]);

    expect(result).toEqual({results: mockResults, fields: mockFields});
    expect(mockQuery).toHaveBeenCalledWith('SELECT * FROM users WHERE id = ?', [1]);
    expect(mockEnd).toHaveBeenCalled();
  });

  it('should handle null and undefined parameters', async () => {
    const mockResults: any[] = [];
    const mockFields: any[] = [];
    mockQuery.mockResolvedValue([mockResults, mockFields]);

    await queryTable(mockConfig, 'INSERT INTO users VALUES (?, ?)', [null, undefined]);

    expect(mockQuery).toHaveBeenCalledWith('INSERT INTO users VALUES (?, ?)', [null, null]);
    expect(mockEnd).toHaveBeenCalled();
  });

  it('should prepare number values correctly', async () => {
    const mockResults: any[] = [];
    const mockFields: any[] = [];
    mockQuery.mockResolvedValue([mockResults, mockFields]);

    await queryTable(mockConfig, 'INSERT INTO users VALUES (?)', [123]);

    expect(mockQuery).toHaveBeenCalledWith('INSERT INTO users VALUES (?)', [123]);
  });

  it('should prepare boolean values correctly', async () => {
    const mockResults: any[] = [];
    const mockFields: any[] = [];
    mockQuery.mockResolvedValue([mockResults, mockFields]);

    await queryTable(mockConfig, 'INSERT INTO users VALUES (?)', [true]);

    expect(mockQuery).toHaveBeenCalledWith('INSERT INTO users VALUES (?)', [true]);
  });

  it('should prepare array values correctly', async () => {
    const mockResults: any[] = [];
    const mockFields: any[] = [];
    mockQuery.mockResolvedValue([mockResults, mockFields]);

    await queryTable(mockConfig, 'INSERT INTO users VALUES (?)', [[1, 2, 3]]);

    expect(mockQuery).toHaveBeenCalledWith('INSERT INTO users VALUES (?)', ['[1,2,3]']);
  });

  it('should prepare date values correctly', async () => {
    const mockResults: any[] = [];
    const mockFields: any[] = [];
    mockQuery.mockResolvedValue([mockResults, mockFields]);

    const testDate = new Date('2023-01-01T12:00:00Z');
    await queryTable(mockConfig, 'INSERT INTO users VALUES (?)', [testDate]);

    expect(mockQuery).toHaveBeenCalled();
    const calledArgs = mockQuery.mock.calls[0][1];
    expect(typeof calledArgs[0]).toBe('string');
  });

  it('should prepare string values correctly', async () => {
    const mockResults: any[] = [];
    const mockFields: any[] = [];
    mockQuery.mockResolvedValue([mockResults, mockFields]);

    await queryTable(mockConfig, 'INSERT INTO users VALUES (?)', ['test string']);

    expect(mockQuery).toHaveBeenCalledWith('INSERT INTO users VALUES (?)', ['test string']);
  });

  it('should sanitize SQL query by removing tabs and newlines', async () => {
    const mockResults: any[] = [];
    const mockFields: any[] = [];
    mockQuery.mockResolvedValue([mockResults, mockFields]);

    await queryTable(mockConfig, 'SELECT  *\n\tFROM\tusers\n\tWHERE  id = ?', [1]);

    expect(mockQuery).toHaveBeenCalledWith('SELECT * FROM users WHERE id = ?', [1]);
    expect(mockEnd).toHaveBeenCalled();
  });

  it('should close connection after query execution', async () => {
    const mockResults: any[] = [];
    const mockFields: any[] = [];
    mockQuery.mockResolvedValue([mockResults, mockFields]);

    await queryTable(mockConfig, 'SELECT * FROM users');

    expect(mockEnd).toHaveBeenCalledTimes(1);
  });
});
