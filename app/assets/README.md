# 本地字体与样式

字体来自 Google Fonts 的官方开源仓库，保留 SIL OFL 1.1 许可证；没有复制 Windows 系统字体。页面及图表使用本地字形子集，不需要请求外部字体 CDN。

- 中文：Noto Sans SC，来源 `https://github.com/google/fonts/tree/main/ofl/notosanssc`。
- 英文与数字：IBM Plex Sans，来源 `https://github.com/google/fonts/tree/main/ofl/ibmplexsans`。

衍生子集内部名称分别改为 **Retail Chinese** 和 **Retail Numerals**，避免沿用保留字体名称。仅收录页面/项目文档用字及拉丁字符；上游版权和许可信息保留在字体及同目录 `*-OFL.txt` 中。源文件与子集 SHA-256 见 `fonts.json`，大体积原始 TTF 只保留于忽略的 `var/w10/fonts/`。

由 FontTools 4.60.1（含 WOFF/Brotli 支持）生成 WOFF2，保留上游可变轴，页面使用 400—700 字重、常规字宽。`theme.css` 经 UI 注入字体的 data URL；没有跨站字体或图片请求。图标与关系图均为代码绘制，商品信息完全来自 API，不补造商品图片。
