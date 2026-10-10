import type { CapacitorConfig } from '@capacitor/cli';

const config: CapacitorConfig = {
  appId: 'com.king.trading',
  appName: 'KING Trading',
  webDir: 'dist',
  backgroundColor: '#0a0e14',
  plugins: {
    // توجيه كل طلبات fetch عبر طبقة الجوال الأصلية — يتجاوز قيود CORS في WebView نهائيًا
    CapacitorHttp: { enabled: true },
  },
};

export default config;
