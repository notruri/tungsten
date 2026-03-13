use std::path::Path;
use std::sync::mpsc::Receiver;

use anyhow::Result;
use raw_window_handle::HasWindowHandle;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TrayEvent {
    Show,
    Quit,
}

pub struct Tray {
    inner: platform::Tray,
    receiver: Option<Receiver<TrayEvent>>,
}

impl Tray {
    pub fn new(icon_path: impl AsRef<Path>) -> Result<Self> {
        let (inner, receiver) = platform::Tray::new(icon_path.as_ref())?;
        Ok(Self {
            inner,
            receiver: Some(receiver),
        })
    }

    pub fn disabled() -> Self {
        Self {
            inner: platform::Tray::disabled(),
            receiver: None,
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.is_enabled()
    }

    pub fn take_receiver(&mut self) -> Option<Receiver<TrayEvent>> {
        self.receiver.take()
    }
}

pub fn hide_window(window: &impl HasWindowHandle) -> Result<()> {
    platform::hide_window(window)
}

pub fn show_window(window: &impl HasWindowHandle) -> Result<()> {
    platform::show_window(window)
}

#[cfg(target_os = "windows")]
mod platform {
    use std::path::Path;
    use std::sync::mpsc::{self, Receiver};

    use anyhow::{Context, Result};
    use raw_window_handle::{HasWindowHandle, RawWindowHandle};
    use tray_icon::menu::{Menu, MenuEvent, MenuId, MenuItem};
    use tray_icon::{
        Icon, MouseButton, MouseButtonState, TrayIcon, TrayIconBuilder, TrayIconEvent,
    };
    use windows_sys::Win32::Foundation::HWND;
    use windows_sys::Win32::UI::WindowsAndMessaging::{SW_HIDE, SW_RESTORE, SW_SHOW, ShowWindow};

    use crate::TrayEvent;

    pub enum Tray {
        Disabled,
        Enabled { _icon: TrayIcon },
    }

    impl Tray {
        pub fn new(icon_path: &Path) -> Result<(Self, Receiver<TrayEvent>)> {
            let (sender, receiver) = mpsc::channel();

            let menu = Menu::new();
            let show_item = MenuItem::with_id("show", "show", true, None);
            let quit_item = MenuItem::with_id("quit", "quit", true, None);
            let show_id = show_item.id().clone();
            let quit_id = quit_item.id().clone();

            menu.append(&show_item)
                .context("failed to add show tray menu item")?;
            menu.append(&quit_item)
                .context("failed to add quit tray menu item")?;

            install_menu_handler(sender.clone(), show_id, quit_id);
            install_icon_handler(sender);

            let icon = build_icon(icon_path).context("failed to load tray icon")?;
            let icon = TrayIconBuilder::new()
                .with_id("tungsten")
                .with_menu(Box::new(menu))
                .with_menu_on_left_click(false)
                .with_tooltip("Tungsten")
                .with_icon(icon)
                .build()
                .context("failed to create tray icon")?;

            Ok((Self::Enabled { _icon: icon }, receiver))
        }

        pub fn disabled() -> Self {
            Self::Disabled
        }

        pub fn is_enabled(&self) -> bool {
            matches!(self, Self::Enabled { .. })
        }
    }

    fn install_menu_handler(sender: mpsc::Sender<TrayEvent>, show_id: MenuId, quit_id: MenuId) {
        MenuEvent::set_event_handler(Some(move |event: MenuEvent| {
            let next = if event.id == show_id {
                Some(TrayEvent::Show)
            } else if event.id == quit_id {
                Some(TrayEvent::Quit)
            } else {
                None
            };

            if let Some(next) = next {
                let _ = sender.send(next);
            }
        }));
    }

    fn install_icon_handler(sender: mpsc::Sender<TrayEvent>) {
        TrayIconEvent::set_event_handler(Some(move |event| match event {
            TrayIconEvent::Click {
                button: MouseButton::Left,
                button_state: MouseButtonState::Up,
                ..
            }
            | TrayIconEvent::DoubleClick {
                button: MouseButton::Left,
                ..
            } => {
                let _ = sender.send(TrayEvent::Show);
            }
            _ => {}
        }));
    }

    fn build_icon(icon_path: &Path) -> Result<Icon> {
        Icon::from_path(icon_path, Some((16, 16)))
            .with_context(|| format!("failed to load tray icon from {}", icon_path.display()))
    }

    pub fn hide_window(window: &impl HasWindowHandle) -> Result<()> {
        let hwnd = hwnd(window)?;
        show_window_state(hwnd, SW_HIDE);
        Ok(())
    }

    pub fn show_window(window: &impl HasWindowHandle) -> Result<()> {
        let hwnd = hwnd(window)?;
        show_window_state(hwnd, SW_RESTORE);
        show_window_state(hwnd, SW_SHOW);
        Ok(())
    }

    fn hwnd(window: &impl HasWindowHandle) -> Result<HWND> {
        let handle = window
            .window_handle()
            .map_err(|error| anyhow::anyhow!("failed to get native window handle: {error}"))?;
        match handle.as_raw() {
            RawWindowHandle::Win32(handle) => Ok(handle.hwnd.get() as HWND),
            _ => anyhow::bail!("unsupported window handle for tray operations"),
        }
    }

    fn show_window_state(hwnd: HWND, command: i32) {
        unsafe {
            ShowWindow(hwnd, command);
        }
    }
}

#[cfg(not(target_os = "windows"))]
mod platform {
    use std::path::Path;
    use std::sync::mpsc::{self, Receiver};

    use anyhow::Result;
    use raw_window_handle::HasWindowHandle;

    use crate::TrayEvent;

    pub struct Tray;

    impl Tray {
        pub fn new(_: &Path) -> Result<(Self, Receiver<TrayEvent>)> {
            let (_sender, receiver) = mpsc::channel();
            Ok((Self, receiver))
        }

        pub fn disabled() -> Self {
            Self
        }

        pub fn is_enabled(&self) -> bool {
            false
        }
    }

    pub fn hide_window(_: &impl HasWindowHandle) -> Result<()> {
        Ok(())
    }

    pub fn show_window(_: &impl HasWindowHandle) -> Result<()> {
        Ok(())
    }
}
