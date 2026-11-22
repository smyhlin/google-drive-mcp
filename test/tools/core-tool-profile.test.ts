import assert from 'node:assert/strict';
import test from 'node:test';
import { setupTestServer } from '../helpers/setup-server.js';

const CORE_TOOLS = [
  'search',
  'listFolder',
  'createFolder',
  'moveItem',
  'renameItem',
  'copyFile',
  'deleteItem',
  'addPermission',
  'listPermissions',
  'removePermission',
  'readTextFile',
  'createTextFile',
  'updateTextFile',
  'uploadFile',
  'createUploadUrl',
  'createDownloadUrl',
].sort();

test('core profile limits tools/list but preserves direct calls to hidden tools', async () => {
  const oldProfile = process.env.GOOGLE_DRIVE_MCP_TOOL_PROFILE;
  process.env.GOOGLE_DRIVE_MCP_TOOL_PROFILE = 'core';
  const ctx = await setupTestServer();
  try {
    const tools = (await ctx.client.listTools()).tools;
    assert.deepEqual(tools.map((tool) => tool.name).sort(), CORE_TOOLS);
    const uploadFile = tools.find((tool) => tool.name === 'uploadFile');
    assert.ok(uploadFile);
    const properties = uploadFile.inputSchema.properties as Record<string, unknown>;
    assert.ok('contentBase64' in properties);
    assert.equal('localPath' in properties, false);
    assert.deepEqual(uploadFile.inputSchema.required, ['contentBase64']);

    const hiddenToolResult = await ctx.client.callTool({ name: 'readGoogleDoc', arguments: {} });
    const content = hiddenToolResult.content;
    assert.ok(Array.isArray(content));
    const errorText = content
      .filter((item: unknown): item is { type: string; text: string } =>
        !!item && typeof item === 'object' && 'type' in item && item.type === 'text' &&
        'text' in item && typeof item.text === 'string',
      )
      .map((item) => item.text)
      .join('\\n');
    assert.equal(hiddenToolResult.isError, true);
    assert.equal(/Tool not found/.test(errorText), false);
  } finally {
    await ctx.cleanup();
    if (oldProfile === undefined) delete process.env.GOOGLE_DRIVE_MCP_TOOL_PROFILE;
    else process.env.GOOGLE_DRIVE_MCP_TOOL_PROFILE = oldProfile;
  }
});
