use gpui::{App, Window};
use gpui_component::{Theme, ThemeMode};

pub use tungsten_client::{AppSettings, SettingsStore, ThemePreference};

pub trait ThemePreferenceExt {
    fn apply(self, window: Option<&mut Window>, cx: &mut App);
}

impl ThemePreferenceExt for ThemePreference {
    fn apply(self, window: Option<&mut Window>, cx: &mut App) {
        let current = Theme::global(cx);
        let font_family = current.font_family.clone();
        let font_size = current.font_size;
        let mono_font_family = current.mono_font_family.clone();
        let mono_font_size = current.mono_font_size;

        match self {
            ThemePreference::System => Theme::sync_system_appearance(window, cx),
            ThemePreference::Light => Theme::change(ThemeMode::Light, window, cx),
            ThemePreference::Dark => Theme::change(ThemeMode::Dark, window, cx),
        }

        let theme = Theme::global_mut(cx);
        theme.font_family = font_family;
        theme.font_size = font_size;
        theme.mono_font_family = mono_font_family;
        theme.mono_font_size = mono_font_size;
    }
}
