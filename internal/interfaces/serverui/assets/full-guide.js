(() => {
  const root = document.getElementById('guide');
  if (!root) return;

  const escapeHTML = (value) => String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');

  const inline = (value) => {
    let text = escapeHTML(value);
    const tokens = [];
    const keep = (html) => {
      const token = `@@EAVC${tokens.length}@@`;
      tokens.push(html);
      return token;
    };
    text = text.replace(/!\[([^\]]*)\]\(([^)]+)\)/g, (_, alt, src) => keep(
      `<figure class="guide-shot"><img loading="lazy" src="${src}" alt="${alt}"><figcaption>${alt}</figcaption></figure>`,
    ));
    text = text.replace(/`([^`]+)`/g, (_, code) => keep(`<code>${code}</code>`));
    text = text.replace(/\[([^\]]+)\]\(([^)]+)\)/g, (_, label, href) => keep(`<a href="${href}" target="_blank" rel="noopener">${label}</a>`));
    text = text.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
    tokens.forEach((html, index) => { text = text.replace(`@@EAVC${index}@@`, html); });
    return text;
  };

  const renderMarkdown = (markdown) => {
    const lines = markdown.replaceAll('\r\n', '\n').split('\n');
    const output = [];
    let sectionOpen = false;
    let listType = '';
    let paragraph = [];
    let quote = [];
    let code = [];
    let codeLanguage = '';
    let inCode = false;
    let skippedDocumentTitle = false;

    const flushParagraph = () => {
      if (!paragraph.length) return;
      output.push(`<p>${inline(paragraph.join(' '))}</p>`);
      paragraph = [];
    };
    const flushQuote = () => {
      if (!quote.length) return;
      output.push(`<blockquote><p>${inline(quote.join(' '))}</p></blockquote>`);
      quote = [];
    };
    const closeList = () => {
      if (!listType) return;
      output.push(`</${listType}>`);
      listType = '';
    };
    const flushAll = () => { flushParagraph(); flushQuote(); closeList(); };

    for (const rawLine of lines) {
      const line = rawLine.trimEnd();
      if (line.startsWith('```')) {
        flushAll();
        if (inCode) {
          if (codeLanguage !== 'mermaid') {
            output.push(`<pre><code>${escapeHTML(code.join('\n'))}</code></pre>`);
          }
          code = [];
          codeLanguage = '';
          inCode = false;
        } else {
          inCode = true;
          codeLanguage = line.slice(3).trim().toLowerCase();
        }
        continue;
      }
      if (inCode) {
        code.push(rawLine);
        continue;
      }
      const heading = line.match(/^(#{1,4})\s+(.+)$/);
      if (heading) {
        flushAll();
        const level = heading[1].length;
        if (level === 1 && !skippedDocumentTitle) {
          skippedDocumentTitle = true;
          continue;
        }
        if (level === 1) {
          if (sectionOpen) output.push('</section>');
          output.push(`<section class="guide-stage"><h1>${inline(heading[2])}</h1>`);
          sectionOpen = true;
        } else {
          if (!sectionOpen) {
            output.push('<section class="guide-stage">');
            sectionOpen = true;
          }
          output.push(`<h${level}>${inline(heading[2])}</h${level}>`);
        }
        continue;
      }
      if (!line.trim()) {
        flushAll();
        continue;
      }
      if (line.startsWith('>')) {
        flushParagraph();
        closeList();
        quote.push(line.replace(/^>\s?/, ''));
        continue;
      }
      const unordered = line.match(/^[-*]\s+(.+)$/);
      const ordered = line.match(/^(\d+)\.\s+(.+)$/);
      if (unordered || ordered) {
        flushParagraph();
        flushQuote();
        const nextType = unordered ? 'ul' : 'ol';
        if (listType !== nextType) {
          closeList();
          output.push(ordered ? `<ol start="${ordered[1]}">` : '<ul>');
          listType = nextType;
        }
        output.push(`<li>${inline(unordered ? unordered[1] : ordered[2])}</li>`);
        continue;
      }
      flushQuote();
      closeList();
      if (/^!\[[^\]]*\]\([^)]+\)$/.test(line.trim())) {
        flushParagraph();
        output.push(inline(line.trim()));
      } else {
        paragraph.push(line.trim());
      }
    }
    flushAll();
    if (inCode && codeLanguage !== 'mermaid') {
      output.push(`<pre><code>${escapeHTML(code.join('\n'))}</code></pre>`);
    }
    if (sectionOpen) output.push('</section>');
    return output.join('\n');
  };

  fetch(root.dataset.source, { cache: 'no-store' })
    .then((response) => {
      if (!response.ok) throw new Error(`HTTP ${response.status}`);
      return response.text();
    })
    .then((markdown) => { root.innerHTML = renderMarkdown(markdown); })
    .catch((error) => {
      root.innerHTML = `<section class="guide-stage load-error"><h1>Инструкция не загрузилась</h1><p>${escapeHTML(error.message)}</p></section>`;
    });
})();
