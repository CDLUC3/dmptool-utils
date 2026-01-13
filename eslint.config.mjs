// @ts-check
import eslint from '@eslint/js';
import tseslint from 'typescript-eslint';

export default tseslint.config(
  eslint.configs.recommended,
  ...tseslint.configs.strict,
  ...tseslint.configs.stylistic,
  {
    rules: {
      // you can also disable specific rules for a file pattern here
      "@typescript-eslint/no-explicit-any": "off"
    }
  }
);
