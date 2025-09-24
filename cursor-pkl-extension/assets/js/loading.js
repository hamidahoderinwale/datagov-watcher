/**
 * Loading and FOUC prevention utilities
 * Handles stylesheet loading tracking and loading overlay management
 */

// Track stylesheet loading to prevent FOUC
let stylesheetsLoaded = 0;
const totalStylesheets = 3; // critical.css, variables.css, and main.css

function checkStylesheetsLoaded() {
  stylesheetsLoaded++;
  console.log(`Stylesheet loaded: ${stylesheetsLoaded}/${totalStylesheets}`);
  
  if (stylesheetsLoaded >= totalStylesheets) {
    document.body.classList.add('stylesheets-loaded');
    hideLoadingOverlay();
  }
}

function hideLoadingOverlay() {
  setTimeout(() => {
    const loadingOverlay = document.getElementById('loadingOverlay');
    if (loadingOverlay) {
      loadingOverlay.classList.add('hidden');
      console.log('Loading overlay hidden');
      
      // Remove from DOM after transition
      setTimeout(() => {
        loadingOverlay.remove();
        console.log('Loading overlay removed from DOM');
      }, 300);
    }
  }, 500); // Small delay to ensure smooth loading experience
}

// Fallback in case onload doesn't fire
window.addEventListener('load', function() {
  console.log('Window load event fired');
  setTimeout(() => {
    document.body.classList.add('stylesheets-loaded');
    hideLoadingOverlay();
  }, 100);
});

// Export functions for potential external use
if (typeof window !== 'undefined') {
  window.checkStylesheetsLoaded = checkStylesheetsLoaded;
  window.hideLoadingOverlay = hideLoadingOverlay;
}
