/**
 * Stats Service Website - Main JavaScript
 */

// ============================================
// Theme Selector
// ============================================
const THEMES = ['neon-cyber', 'sunset', 'arctic', 'forest', 'midnight', 'solar'];
const DEFAULT_THEME = 'neon-cyber';

function initThemeSelector() {
  const selector = document.getElementById('theme-selector');
  if (!selector) return;

  // Load saved theme or use default
  const savedTheme = localStorage.getItem('stats-theme') || DEFAULT_THEME;
  setTheme(savedTheme);
  selector.value = savedTheme;

  // Listen for changes
  selector.addEventListener('change', (e) => {
    const theme = e.target.value;
    setTheme(theme);
    localStorage.setItem('stats-theme', theme);
  });
}

function setTheme(theme) {
  if (!THEMES.includes(theme)) {
    theme = DEFAULT_THEME;
  }
  document.documentElement.setAttribute('data-theme', theme);

  // Update all theme selectors on the page (for docs pages)
  document.querySelectorAll('.theme-selector').forEach(sel => {
    sel.value = theme;
  });
}

// Apply theme immediately to prevent flash
(function() {
  const savedTheme = localStorage.getItem('stats-theme') || DEFAULT_THEME;
  document.documentElement.setAttribute('data-theme', savedTheme);
})();

// ============================================
// Copy Code Button
// ============================================
function copyCode(button) {
  const codeBlock = button.closest('.code-block');
  const code = codeBlock.querySelector('pre code').textContent;

  navigator.clipboard.writeText(code).then(() => {
    const originalText = button.innerHTML;
    button.innerHTML = '&#10003; Copied!';
    button.style.color = '#10b981';

    setTimeout(() => {
      button.innerHTML = originalText;
      button.style.color = '';
    }, 2000);
  }).catch(err => {
    console.error('Failed to copy:', err);
  });
}

// ============================================
// Mobile Menu Toggle
// ============================================
function initMobileMenu() {
  const mobileMenuBtn = document.querySelector('.mobile-menu-btn');
  const navbarNav = document.querySelector('.navbar-nav');

  if (mobileMenuBtn && navbarNav) {
    mobileMenuBtn.addEventListener('click', () => {
      navbarNav.classList.toggle('mobile-open');
      const isOpen = navbarNav.classList.contains('mobile-open');
      mobileMenuBtn.innerHTML = isOpen ? '&#10005;' : '&#9776;';
    });
  }
}

// ============================================
// Smooth Scroll for Anchor Links
// ============================================
function initSmoothScroll() {
  document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function (e) {
      e.preventDefault();
      const target = document.querySelector(this.getAttribute('href'));
      if (target) {
        target.scrollIntoView({
          behavior: 'smooth',
          block: 'start'
        });
      }
    });
  });
}

// ============================================
// Navbar Background on Scroll
// ============================================
function initNavbarScroll() {
  const navbar = document.querySelector('.navbar');
  if (!navbar) return;

  window.addEventListener('scroll', () => {
    if (window.scrollY > 50) {
      navbar.classList.add('scrolled');
    } else {
      navbar.classList.remove('scrolled');
    }
  });
}

// ============================================
// Active Sidebar Link
// ============================================
function initActiveSidebarLink() {
  const sidebarLinks = document.querySelectorAll('.sidebar-nav a');
  const currentPath = window.location.pathname;

  sidebarLinks.forEach(link => {
    if (link.getAttribute('href') === currentPath ||
        currentPath.endsWith(link.getAttribute('href'))) {
      link.classList.add('active');
    }
  });
}

// ============================================
// Code Syntax Highlighting (Basic)
// ============================================
function highlightCode() {
  document.querySelectorAll('pre code').forEach(block => {
    let html = block.innerHTML;

    // Comments
    html = html.replace(/(#[^\n]*)/g, '<span class="token-comment">$1</span>');

    // Strings
    html = html.replace(/('[^']*')/g, '<span class="token-string">$1</span>');
    html = html.replace(/("[^"]*")/g, '<span class="token-string">$1</span>');

    // Keywords
    const keywords = ['SELECT', 'FROM', 'WHERE', 'GROUP BY', 'ORDER BY', 'LIMIT',
                      'JOIN', 'LEFT', 'RIGHT', 'INNER', 'ON', 'AND', 'OR', 'AS',
                      'AVG', 'COUNT', 'MIN', 'MAX', 'SUM', 'HAVING', 'BETWEEN',
                      'INSERT', 'UPDATE', 'DELETE', 'CREATE', 'DROP', 'ALTER'];
    keywords.forEach(kw => {
      const regex = new RegExp(`\\b(${kw})\\b`, 'gi');
      html = html.replace(regex, '<span class="token-keyword">$1</span>');
    });

    // Numbers
    html = html.replace(/\b(\d+\.?\d*)\b/g, '<span class="token-number">$1</span>');

    block.innerHTML = html;
  });
}

// ============================================
// Table of Contents Generator
// ============================================
function generateTOC() {
  const content = document.querySelector('.docs-content');
  const toc = document.querySelector('.table-of-contents');

  if (!content || !toc) return;

  const headings = content.querySelectorAll('h2, h3');
  const tocList = document.createElement('ul');

  headings.forEach((heading, index) => {
    // Add ID to heading if not present
    if (!heading.id) {
      heading.id = `heading-${index}`;
    }

    const li = document.createElement('li');
    li.className = heading.tagName === 'H3' ? 'toc-h3' : 'toc-h2';

    const link = document.createElement('a');
    link.href = `#${heading.id}`;
    link.textContent = heading.textContent;
    link.addEventListener('click', (e) => {
      e.preventDefault();
      heading.scrollIntoView({ behavior: 'smooth' });
    });

    li.appendChild(link);
    tocList.appendChild(li);
  });

  toc.appendChild(tocList);
}

// ============================================
// Search Functionality (Basic)
// ============================================
function initSearch() {
  const searchInput = document.querySelector('.search-input');
  if (!searchInput) return;

  searchInput.addEventListener('input', (e) => {
    const query = e.target.value.toLowerCase();
    const sidebarLinks = document.querySelectorAll('.sidebar-nav a');

    sidebarLinks.forEach(link => {
      const text = link.textContent.toLowerCase();
      const li = link.parentElement;

      if (text.includes(query) || query === '') {
        li.style.display = '';
      } else {
        li.style.display = 'none';
      }
    });
  });
}

// ============================================
// Tab Switching
// ============================================
function initTabs() {
  const tabButtons = document.querySelectorAll('.tab-btn');
  const tabPanels = document.querySelectorAll('.tab-panel');

  tabButtons.forEach(button => {
    button.addEventListener('click', () => {
      const target = button.dataset.tab;

      // Remove active class from all buttons and panels
      tabButtons.forEach(btn => btn.classList.remove('active'));
      tabPanels.forEach(panel => panel.classList.remove('active'));

      // Add active class to clicked button and corresponding panel
      button.classList.add('active');
      document.querySelector(`#${target}`).classList.add('active');
    });
  });
}

// ============================================
// Collapsible Sections
// ============================================
function initCollapsible() {
  const collapsibles = document.querySelectorAll('.collapsible-header');

  collapsibles.forEach(header => {
    header.addEventListener('click', () => {
      const content = header.nextElementSibling;
      const isOpen = header.classList.contains('open');

      header.classList.toggle('open');
      content.style.maxHeight = isOpen ? '0' : content.scrollHeight + 'px';
    });
  });
}

// ============================================
// Animate on Scroll
// ============================================
function initScrollAnimations() {
  const observerOptions = {
    threshold: 0.1,
    rootMargin: '0px 0px -50px 0px'
  };

  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        entry.target.classList.add('animate-fade-in');
        observer.unobserve(entry.target);
      }
    });
  }, observerOptions);

  // Observe feature cards and other animatable elements
  document.querySelectorAll('.feature-card, .card, .arch-card, .stat-item').forEach(el => {
    el.style.opacity = '0';
    observer.observe(el);
  });
}

// ============================================
// Initialize on DOM Load
// ============================================
document.addEventListener('DOMContentLoaded', () => {
  initThemeSelector();
  initMobileMenu();
  initSmoothScroll();
  initNavbarScroll();
  initActiveSidebarLink();
  generateTOC();
  initSearch();
  initTabs();
  initCollapsible();
  initScrollAnimations();
});

// ============================================
// Version Selector
// ============================================
function initVersionSelector() {
  const selector = document.querySelector('.version-selector');
  if (!selector) return;

  selector.addEventListener('change', (e) => {
    const version = e.target.value;
    // Handle version change - could redirect to versioned docs
    console.log('Selected version:', version);
  });
}
