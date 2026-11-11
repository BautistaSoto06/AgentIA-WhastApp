import { createApp } from '../src/app';

describe('App Setup', () => {
  it('should create app successfully', () => {
    const app = createApp();
    expect(app).toBeDefined();
    expect(app).toHaveProperty('get');
    expect(app).toHaveProperty('post');
    expect(app).toHaveProperty('use');
  });
});
