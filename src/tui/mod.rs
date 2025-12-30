mod exporter;
mod stats;

use std::{
    io,
    sync::{Arc, Mutex},
    time::Duration,
};

pub use exporter::*;
use duckdb::arrow::array::ArrowNativeTypeOp;
use ratatui::{
    Frame,
    crossterm::event::{self, Event, KeyCode, KeyEvent, KeyEventKind},
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Gauge, Paragraph, Widget},
};
pub use stats::*;
use throbber_widgets_tui::{Throbber, ThrobberState, BRAILLE_SIX};
use tokio::task::JoinHandle;
use tui_logger::TuiLoggerWidget;

pub struct Tui {
    stats: Arc<Mutex<Stats>>,
    exit: bool,
    fetching_throbber_state: ThrobberState,
    indexing_throbber_state: ThrobberState,
}

impl Tui {
    pub fn spawn(stats: Arc<Mutex<Stats>>) -> JoinHandle<()> {
        let mut terminal = ratatui::init();
        let mut tui = Self {
            stats,
            exit: false,
            fetching_throbber_state: ThrobberState::default(),
            indexing_throbber_state: ThrobberState::default(),
        };

        tokio::task::spawn_blocking(move || {
            while !tui.exit {
                if let Err(_) = terminal.draw(|frame| tui.draw(frame)) {
                    break;
                }
                if let Err(_) = tui.handle_events() {
                    break;
                }
                tui.update();
            }
            ratatui::restore();
        })
    }

    fn draw(&self, frame: &mut Frame) {
        frame.render_widget(self, frame.area());
    }

    fn handle_events(&mut self) -> io::Result<()> {
        if !event::poll(Duration::from_millis(200))? {
            return Ok(());
        }

        if let Event::Key(key_event) = event::read()?
            && key_event.kind == KeyEventKind::Press
        {
            self.handle_key_event(key_event);
        }

        Ok(())
    }

    fn handle_key_event(&mut self, key_event: KeyEvent) {
        match key_event.code {
            KeyCode::Char('q') => self.exit = true,
            _ => {}
        }
    }

    fn update(&mut self) {
        self.fetching_throbber_state.calc_next();
        self.indexing_throbber_state.calc_next();
        // Move log events from hot buffer to widget buffer
        tui_logger::move_events();
    }
}

impl Widget for &Tui {
    fn render(self, area: ratatui::prelude::Rect, buf: &mut ratatui::prelude::Buffer)
    where
        Self: Sized,
    {
        let snapshot = self.stats.lock().unwrap().snapshot();

        let fetching_progress = (snapshot.last_fetched_block as f64)
            .div_checked(snapshot.current_head_number as f64)
            .unwrap_or(0.0);
        let indexing_progress = (snapshot.last_saved_block as f64)
            .div_checked(snapshot.current_head_number as f64)
            .unwrap_or(0.0);

        // Main layout: stats grid on top, logs in middle, progress bars and footer at bottom
        let main_layout = Layout::default()
            .direction(Direction::Vertical)
            .margin(1)
            .constraints([
                Constraint::Length(8), // Stats grid (2 rows of 4 height each)
                Constraint::Min(6),    // Logs
                Constraint::Length(3), // Fetching progress
                Constraint::Length(3), // Indexing progress
                Constraint::Length(1), // Footer
            ])
            .split(area);

        // Render stats boxes
        render_stats_grid(buf, main_layout[0], &snapshot);

        // Render logs
        render_logs(buf, main_layout[1]);

        // Render fetching progress bar
        render_progress_bar(
            buf,
            main_layout[2],
            "Fetching",
            fetching_progress,
            snapshot.last_fetched_block,
            snapshot.current_head_number,
            Color::Cyan,
        );

        // Render indexing progress bar
        render_progress_bar(
            buf,
            main_layout[3],
            "Indexing",
            indexing_progress,
            snapshot.last_saved_block,
            snapshot.current_head_number,
            Color::Green,
        );

        // Render footer with spinners and version
        render_footer(
            buf,
            main_layout[4],
            &self.fetching_throbber_state,
            &self.indexing_throbber_state,
        );
    }
}

fn render_progress_bar(
    buf: &mut ratatui::prelude::Buffer,
    area: Rect,
    title: &str,
    progress: f64,
    current: u64,
    total: u64,
    color: Color,
) {
    let label = format!("{}/{} ({:.1}%)", current, total, progress * 100.0);
    let gauge = Gauge::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray))
                .title(Span::styled(
                    format!(" {} ", title),
                    Style::default().fg(color).add_modifier(Modifier::BOLD),
                )),
        )
        .gauge_style(Style::default().fg(color).bg(Color::Black))
        .ratio(progress.clamp(0.0, 1.0))
        .label(Span::styled(label, Style::default().fg(Color::White)));

    gauge.render(area, buf);
}

fn render_stats_grid(buf: &mut ratatui::prelude::Buffer, area: Rect, snapshot: &StatsSnapshot) {
    // Create a 2-row layout for stats
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(4), // First row of stats
            Constraint::Length(4), // Second row of stats
        ])
        .split(area);

    // First row: 3 stats boxes
    let row1_cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Ratio(1, 3),
            Constraint::Ratio(1, 3),
            Constraint::Ratio(1, 3),
        ])
        .split(rows[0]);

    // Second row: 3 stats boxes
    let row2_cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Ratio(1, 3),
            Constraint::Ratio(1, 3),
            Constraint::Ratio(1, 3),
        ])
        .split(rows[1]);

    // Format values
    let uptime_str = format_duration(snapshot.uptime_seconds);
    let eta_str = match snapshot.sync_eta {
        None => "Synced".to_string(),
        Some(eta) if eta == u64::MAX => "Calculating...".to_string(),
        Some(eta) => format_duration(eta),
    };
    let p99_str = snapshot
        .block_fetch_p99_ms
        .map(|ms| format!("{:.1}ms", ms))
        .unwrap_or_else(|| "-".to_string());
    let (rpc_status, rpc_color) = if snapshot.rpc_connection_healthy {
        ("Healthy", Color::Green)
    } else {
        ("Down", Color::Red)
    };

    // Row 1: RPC, Uptime, ETA
    render_stat_box(buf, row1_cols[0], "RPC", rpc_status, rpc_color);
    render_stat_box(buf, row1_cols[1], "Uptime", &uptime_str, Color::Yellow);
    render_stat_box(buf, row1_cols[2], "ETA", &eta_str, Color::Magenta);

    // Row 2: Fetch Rate, Fetch p99, Reorgs
    render_stat_box(
        buf,
        row2_cols[0],
        "Fetch Rate",
        &format!("{:.1} blk/s", snapshot.fetch_rps),
        Color::Cyan,
    );
    render_stat_box(buf, row2_cols[1], "Fetch p99", &p99_str, Color::Blue);
    render_stat_box(
        buf,
        row2_cols[2],
        "Reorgs",
        &snapshot.reorgs_detected_total.to_string(),
        if snapshot.reorgs_detected_total > 0 {
            Color::Yellow
        } else {
            Color::White
        },
    );
}

fn render_stat_box(
    buf: &mut ratatui::prelude::Buffer,
    area: Rect,
    label: &str,
    value: &str,
    color: Color,
) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::DarkGray))
        .title(Span::styled(
            format!(" {} ", label),
            Style::default()
                .fg(Color::Gray)
                .add_modifier(Modifier::BOLD),
        ));

    let inner = block.inner(area);
    block.render(area, buf);

    // Render value centered in the box
    let value_line = Line::from(Span::styled(
        value,
        Style::default().fg(color).add_modifier(Modifier::BOLD),
    ));

    let paragraph = Paragraph::new(value_line).alignment(Alignment::Center);

    // Center vertically
    if inner.height > 0 {
        let centered_area = Rect {
            x: inner.x,
            y: inner.y + (inner.height.saturating_sub(1)) / 2,
            width: inner.width,
            height: 1,
        };
        paragraph.render(centered_area, buf);
    }
}

fn render_footer(
    buf: &mut ratatui::prelude::Buffer,
    area: Rect,
    fetching_state: &ThrobberState,
    indexing_state: &ThrobberState,
) {
    // Create throbbers for fetching and indexing
    let fetching_throbber = Throbber::default()
        .throbber_set(BRAILLE_SIX)
        .throbber_style(
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        );
    let indexing_throbber = Throbber::default()
        .throbber_set(BRAILLE_SIX)
        .throbber_style(
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::BOLD),
        );

    // Build footer line with spinners on left and version on right
    let fetching_symbol = fetching_throbber.to_symbol_span(fetching_state);
    let indexing_symbol = indexing_throbber.to_symbol_span(indexing_state);

    let footer_line = Line::from(vec![
        fetching_symbol,
        Span::styled(" Fetching  ", Style::default().fg(Color::DarkGray)),
        indexing_symbol,
        Span::styled(" Indexing", Style::default().fg(Color::DarkGray)),
    ]);

    let version = Line::from(Span::styled(
        "weifinder v0.0.1",
        Style::default().fg(Color::DarkGray),
    ));

    // Render spinners on the left
    let left_paragraph = Paragraph::new(footer_line);
    left_paragraph.render(area, buf);

    // Render version on the right
    let right_paragraph = Paragraph::new(version).alignment(Alignment::Right);
    right_paragraph.render(area, buf);
}

fn render_logs(buf: &mut ratatui::prelude::Buffer, area: Rect) {
    let log_widget = TuiLoggerWidget::default()
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(Color::DarkGray))
                .title(Span::styled(
                    " Logs ",
                    Style::default()
                        .fg(Color::White)
                        .add_modifier(Modifier::BOLD),
                )),
        )
        .style(Style::default().fg(Color::White));

    log_widget.render(area, buf);
}

fn format_duration(seconds: u64) -> String {
    let hours = seconds / 3600;
    let minutes = (seconds % 3600) / 60;
    let secs = seconds % 60;

    if hours > 0 {
        format!("{}h {:02}m", hours, minutes)
    } else if minutes > 0 {
        format!("{}m {:02}s", minutes, secs)
    } else {
        format!("{}s", secs)
    }
}
