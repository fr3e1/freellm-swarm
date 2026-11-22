// Copy-to-clipboard buttons for code blocks
// No dependencies, no inline handlers, minimal footprint

(function () {
  'use strict';

  function createCopyButton() {
    const btn = document.createElement('button');
    btn.type = 'button';
    btn.className = 'copy-btn';
    btn.textContent = 'Copy';
    btn.setAttribute('aria-label', 'Copy code to clipboard');
    return btn;
  }

  function showCopied(btn) {
    btn.textContent = 'Copied!';
    btn.classList.add('copied');
    setTimeout(() => {
      btn.textContent = 'Copy';
      btn.classList.remove('copied');
    }, 2000);
  }

  function copyToClipboard(text, btn) {
    if (navigator.clipboard && navigator.clipboard.writeText) {
      navigator.clipboard.writeText(text).then(
        () => showCopied(btn),
        () => fallbackCopy(text, btn)
      );
    } else {
      fallbackCopy(text, btn);
    }
  }

  function fallbackCopy(text, btn) {
    const textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.style.position = 'fixed';
    textarea.style.opacity = '0';
    document.body.appendChild(textarea);
    textarea.select();
    try {
      document.execCommand('copy');
      showCopied(btn);
    } catch (_) {
      btn.textContent = 'Failed';
      setTimeout(() => { btn.textContent = 'Copy'; }, 2000);
    }
    document.body.removeChild(textarea);
  }

  function init() {
    const codeBlocks = document.querySelectorAll('pre code');
    codeBlocks.forEach((code) => {
      const pre = code.parentElement;
      if (!pre || pre.classList.contains('no-copy')) return;
      if (pre.querySelector('.copy-btn')) return;

      const btn = createCopyButton();
      pre.style.position = 'relative';
      pre.appendChild(btn);

      btn.addEventListener('click', (e) => {
        e.stopPropagation();
        const text = code.textContent;
        copyToClipboard(text, btn);
      });
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
