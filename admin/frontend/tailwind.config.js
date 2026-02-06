/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.rs",
    "./index.html",
  ],
  theme: {
    extend: {
      colors: {
        'myna': {
          'primary': '#1a56db',
          'primary-dark': '#1e429f',
          'secondary': '#4b5563',
          'success': '#046c4e',
          'warning': '#c27803',
          'danger': '#c81e1e',
          'info': '#1c64f2',
        }
      },
      fontFamily: {
        'mono': ['ui-monospace', 'SFMono-Regular', 'Monaco', 'Consolas', 'Liberation Mono', 'Courier New', 'monospace'],
      }
    }
  },
  plugins: []
}
