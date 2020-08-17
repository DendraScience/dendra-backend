module.exports = {
  root: true,
  env: {
    node: true,
    commonjs: true,
    es6: true,
    jquery: false,
    jest: true,
    jasmine: true
  },
  extends: ['standard', 'prettier', 'prettier/standard'],
  plugins: ['import', 'prettier', 'standard'],
  parserOptions: {
    sourceType: 'module'
  },
  rules: {
    'prettier/prettier': 'error'
    // 'no-unexpected-multiline': ['warn']
  }
  // extends: 'eslint:recommended',
  // parserOptions: {
  //     sourceType: 'module',
  //     ecmaVersion: '2018'
  // },
  // rules: {
  //     indent: ['warn', 'tab', { SwitchCase: 1 }],
  //     quotes: ['warn', 'double'],
  //     semi: ['error', 'always'],
  //     'no-var': ['error'],
  //     'no-console': ['off'],
  //     'no-unused-vars': ['warn'],
  //     'no-mixed-spaces-and-tabs': ['warn']
  // }
}
