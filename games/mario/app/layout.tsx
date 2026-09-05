import type { Metadata } from 'next';
import './globals.css';
export const metadata: Metadata = { title: '超级马里奥 · 像素冒险', description: '经典 8 位像素风超级马里奥同人小游戏，支持键盘与触屏。' };
export default function RootLayout({ children }: Readonly<{ children: React.ReactNode }>) { return <html lang="zh-CN"><body>{children}</body></html>; }
