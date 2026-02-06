/**
 * MynaDB Website - Main JavaScript
 * Fantastic animations and interactions
 */

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
    button.style.transform = 'scale(1.1)';

    setTimeout(() => {
      button.innerHTML = originalText;
      button.style.color = '';
      button.style.transform = '';
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
// Navbar Effects on Scroll
// ============================================
function initNavbarScroll() {
  const navbar = document.querySelector('.navbar');
  if (!navbar) return;

  let lastScroll = 0;

  window.addEventListener('scroll', () => {
    const currentScroll = window.scrollY;

    // Add scrolled class for background change
    if (currentScroll > 50) {
      navbar.classList.add('scrolled');
    } else {
      navbar.classList.remove('scrolled');
    }

    lastScroll = currentScroll;
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
// Scroll Reveal Animation
// ============================================
function initScrollAnimations() {
  const observerOptions = {
    threshold: 0.1,
    rootMargin: '0px 0px -50px 0px'
  };

  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        entry.target.classList.add('visible');

        // Add staggered animation for children
        const children = entry.target.querySelectorAll('.animate-child');
        children.forEach((child, index) => {
          child.style.transitionDelay = `${index * 0.1}s`;
          child.classList.add('visible');
        });
      }
    });
  }, observerOptions);

  // Observe elements with animation class
  document.querySelectorAll('.animate-on-scroll').forEach(el => {
    observer.observe(el);
  });

  // Auto-add animation class to common elements
  const animatableSelectors = [
    '.feature-card',
    '.card',
    '.arch-card',
    '.stat-item',
    '.section-header'
  ];

  animatableSelectors.forEach(selector => {
    document.querySelectorAll(selector).forEach((el, index) => {
      if (!el.classList.contains('animate-on-scroll')) {
        el.classList.add('animate-on-scroll');
        el.classList.add(`delay-${(index % 4) + 1}`);
        observer.observe(el);
      }
    });
  });
}

// ============================================
// Parallax Effect for Hero
// ============================================
function initParallax() {
  const hero = document.querySelector('.hero');
  const particles = document.querySelector('.hero-particles');

  if (!hero || !particles) return;

  window.addEventListener('scroll', () => {
    const scrolled = window.scrollY;
    const rate = scrolled * 0.3;

    if (scrolled < window.innerHeight) {
      particles.style.transform = `translateY(${rate}px)`;
    }
  });
}

// ============================================
// Mouse Move Effect on Cards
// ============================================
function initCardTilt() {
  const cards = document.querySelectorAll('.feature-card, .card');

  cards.forEach(card => {
    card.addEventListener('mousemove', (e) => {
      const rect = card.getBoundingClientRect();
      const x = e.clientX - rect.left;
      const y = e.clientY - rect.top;

      const centerX = rect.width / 2;
      const centerY = rect.height / 2;

      const rotateX = (y - centerY) / 20;
      const rotateY = (centerX - x) / 20;

      card.style.transform = `perspective(1000px) rotateX(${rotateX}deg) rotateY(${rotateY}deg) translateY(-8px) scale(1.02)`;
    });

    card.addEventListener('mouseleave', () => {
      card.style.transform = '';
    });
  });
}

// ============================================
// Typing Effect for Code
// ============================================
function initTypingEffect() {
  const codeBlocks = document.querySelectorAll('.hero-code pre code');

  codeBlocks.forEach(block => {
    const text = block.innerHTML;
    block.innerHTML = '';
    block.style.visibility = 'visible';

    let i = 0;
    const speed = 10; // ms per character

    function typeWriter() {
      if (i < text.length) {
        block.innerHTML = text.substring(0, i + 1);
        i++;
        setTimeout(typeWriter, speed);
      }
    }

    // Start typing when element is in view
    const observer = new IntersectionObserver((entries) => {
      entries.forEach(entry => {
        if (entry.isIntersecting) {
          setTimeout(typeWriter, 500);
          observer.unobserve(entry.target);
        }
      });
    }, { threshold: 0.5 });

    observer.observe(block.closest('.hero-code'));
  });
}

// ============================================
// Counter Animation for Stats
// ============================================
function initCounterAnimation() {
  const stats = document.querySelectorAll('.stat-item h3');

  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        const target = entry.target;
        const text = target.textContent;

        // Extract number and suffix
        const match = text.match(/^([\d.]+)(.*)$/);
        if (match) {
          const endValue = parseFloat(match[1]);
          const suffix = match[2];
          const duration = 2000;
          const startTime = performance.now();

          function animate(currentTime) {
            const elapsed = currentTime - startTime;
            const progress = Math.min(elapsed / duration, 1);

            // Easing function
            const easeOut = 1 - Math.pow(1 - progress, 3);
            const currentValue = endValue * easeOut;

            if (Number.isInteger(endValue)) {
              target.textContent = Math.floor(currentValue) + suffix;
            } else {
              target.textContent = currentValue.toFixed(1) + suffix;
            }

            if (progress < 1) {
              requestAnimationFrame(animate);
            } else {
              target.textContent = text; // Ensure exact final value
            }
          }

          requestAnimationFrame(animate);
        }

        observer.unobserve(target);
      }
    });
  }, { threshold: 0.5 });

  stats.forEach(stat => observer.observe(stat));
}

// ============================================
// Magnetic Button Effect
// ============================================
function initMagneticButtons() {
  const buttons = document.querySelectorAll('.btn-primary');

  buttons.forEach(button => {
    button.addEventListener('mousemove', (e) => {
      const rect = button.getBoundingClientRect();
      const x = e.clientX - rect.left - rect.width / 2;
      const y = e.clientY - rect.top - rect.height / 2;

      button.style.transform = `translate(${x * 0.2}px, ${y * 0.2}px) translateY(-3px) scale(1.02)`;
    });

    button.addEventListener('mouseleave', () => {
      button.style.transform = '';
    });
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
// Search Functionality
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

      tabButtons.forEach(btn => btn.classList.remove('active'));
      tabPanels.forEach(panel => panel.classList.remove('active'));

      button.classList.add('active');
      const panel = document.querySelector(`#${target}`);
      if (panel) panel.classList.add('active');
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
// Cursor Glow Effect
// ============================================
function initCursorGlow() {
  const hero = document.querySelector('.hero');
  if (!hero) return;

  const glow = document.createElement('div');
  glow.style.cssText = `
    position: fixed;
    width: 400px;
    height: 400px;
    background: radial-gradient(circle, rgba(2, 132, 199, 0.15) 0%, transparent 70%);
    pointer-events: none;
    z-index: 0;
    transform: translate(-50%, -50%);
    transition: opacity 0.3s ease;
    opacity: 0;
  `;
  document.body.appendChild(glow);

  document.addEventListener('mousemove', (e) => {
    if (e.clientY < window.innerHeight) {
      glow.style.left = e.clientX + 'px';
      glow.style.top = e.clientY + 'px';
      glow.style.opacity = '1';
    } else {
      glow.style.opacity = '0';
    }
  });

  document.addEventListener('mouseleave', () => {
    glow.style.opacity = '0';
  });
}

// ============================================
// Initialize Everything
// ============================================
document.addEventListener('DOMContentLoaded', () => {
  initMobileMenu();
  initSmoothScroll();
  initNavbarScroll();
  initActiveSidebarLink();
  initScrollAnimations();
  initParallax();
  initCardTilt();
  initCounterAnimation();
  initMagneticButtons();
  initCursorGlow();
  generateTOC();
  initSearch();
  initTabs();
  initCollapsible();
});

// Disable typing effect by default (can be enabled if desired)
// initTypingEffect();
