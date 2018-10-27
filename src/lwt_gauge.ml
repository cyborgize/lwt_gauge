
open ExtLib
open Printf

let log = Log.from "lwt_gauge"

module Gauge = struct

  type stream_type =
    | From
    | FromDirect
    | Bounded
    | Unbounded
    | UnboundedWithRef
    | OfList
    | OfArray
    | OfString
    | Clone

  let string_of_stream_type = function
    | From -> "from"
    | FromDirect -> "from_direct"
    | Bounded -> "create_bounded"
    | Unbounded -> "create"
    | UnboundedWithRef -> "create_with_reference"
    | OfList -> "of_list"
    | OfArray -> "of_array"
    | OfString -> "of_string"
    | Clone -> "clone"

  type props = {
    type_ : stream_type;
    name : string option;
    mutable count : int;
    mutable size : int option;
  }

  type controls = <
    incr : unit;
    decr : unit;
    resize : int -> unit;
    props : props;
  >

  type gauge = <
    is_closed : bool;
    is_empty : bool;
    controls : controls;
  >

  type global_state = {
    mutable streams : gauge Dllist.node_t option;
  }

  let make'' type_ ~name ?(count=0) ?size () =
    let props = { type_; name; count; size; } in
    object
      method incr = props.count <- props.count + 1
      method decr = props.count <- props.count -1
      method resize n = props.size <- Some n
      method props = props
    end

  let make' controls stream =
    object
      method is_closed = Lwt_stream.is_closed stream
      method is_empty =
        match Lwt.state (Lwt_stream.is_empty stream) with
        | Return is_empty -> is_empty
        | Sleep | Fail _ -> assert false (* if is_closed, is_empty is guaranteed not to block *)
      method controls = controls
    end

  let make type_ ~name ?count ?size stream =
    let controls = make'' type_ ~name ?count ?size () in
    make' controls stream

  let attach g s stream =
    let handle =
      match g.streams with
      | Some streams -> Dllist.prepend streams s
      | None ->
      let streams = Dllist.create s in
      g.streams <- Some streams;
      streams
    in
    let remove () =
      match g.streams with
      | None -> assert false
      | Some streams when streams != handle -> Dllist.remove handle
      | Some _ ->
      let next = Dllist.drop handle in
      g.streams <- if next != handle then Some next else None
    in
    Lwt_stream.from begin fun () ->
      match%lwt Lwt_stream.get stream with
      | Some _ as x -> s #controls #decr; Lwt.return x
      | None -> remove (); Lwt.return_none
    end

  let gauge g type_ ~name ?count ?size stream =
    let s = make type_ ~name ?count ?size stream in
    attach g s stream

  let end_gauge { streams; } =
    match streams with
    | None -> ()
    | Some streams ->
    let streams = Dllist.to_list streams in
    log #warn "%d streams are still active:" (List.length streams);
    List.iteri begin fun i s ->
      let attrs =
        match s #is_closed with
        | false -> []
        | true ->
        "closed" ::
        match s #is_empty with
        | false -> []
        | true ->
        "empty" :: []
      in
      let { type_; name; count; size; } = s #controls #props in
      let attrs =
        begin match size with
        | Some size -> sprintf "%d/%d" count size
        | None -> string_of_int count
        end ::
        attrs
      in
      log #warn "%4d) Lwt_stream.%s ~name:%S (* %s *)"
        (i + 1) (string_of_stream_type type_) (Option.default "<unnamed>" name) (String.concat ", " attrs)
    end streams

end

module Lwt_stream = struct

  include Lwt_stream

  open Gauge

  let tls = Lwt.new_key ()

  let from ?name f =
    match Lwt.get tls with
    | None -> from f
    | Some g ->
    let controls = make'' From ~name () in
    let f () =
      match%lwt f () with
      | Some _ as x -> controls #incr; Lwt.return x
      | None -> Lwt.return_none
    in
    let s = from f in
    let s' = make' controls s in
    attach g s' s

  let from_direct ?name f =
    match Lwt.get tls with
    | None -> from_direct f
    | Some g ->
    let controls = make'' FromDirect ~name () in
    let f () =
      match f () with
      | Some _ as x -> controls #incr; x
      | None -> None
    in
    let s = from_direct f in
    let s' = make' controls s in
    attach g s' s

  let create ?name () =
    match Lwt.get tls with
    | None -> create ()
    | Some g ->
    let (s, f) = create () in
    let s' = make Unbounded ~name s in
    let f = function Some _ as x -> s' #controls #incr; f x | None -> f None in
    attach g s' s, f

  let create_with_reference ?name () =
    match Lwt.get tls with
    | None -> create_with_reference ()
    | Some g ->
    let (s, f, r) = create_with_reference () in
    let s' = make Unbounded ~name s in
    let f = function Some _ as x -> s' #controls #incr; f x | None -> f None in
    attach g s' s, f, r

  let create_bounded ?name n =
    match Lwt.get tls with
    | None -> create_bounded n
    | Some g ->
    let (s, p) = create_bounded n in
    let s' = make Bounded ~name ~size:n s in
    let s = attach g s' s in
    s, object
      method size = p #size
      method resize n = s' #controls #resize n; p #resize n
      method push x = let%lwt () = p #push x in s' #controls #incr; Lwt.return_unit
      method close = p #close
      method count = p #count
      method blocked = p #blocked
      method closed = p #closed
      method set_reference : 'a. 'a -> unit = fun x -> p #set_reference x
    end

  let of_list ?name l =
    match Lwt.get tls with
    | None -> of_list l
    | Some g ->
    let n = List.length l in
    gauge g OfList ~name ~count:n ~size:n (of_list l)

  let of_array ?name a =
    match Lwt.get tls with
    | None -> of_array a
    | Some g ->
    let n = Array.length a in
    gauge g OfArray ~name ~count:n ~size:n (of_array a)

  let of_string ?name s =
    match Lwt.get tls with
    | None -> of_string s
    | Some g ->
    let n = String.length s in
    gauge g OfString ~name ~count:n ~size:n (of_string s)

  let clone ?name s =
    match Lwt.get tls with
    | None -> clone s
    | Some g -> gauge g Clone ~name (clone s)

  let with_gauge f =
    let gauge = { streams = None; } in
    Lwt.with_value tls (Some gauge) @@ fun () ->
    (f ()) [%lwt.finally end_gauge gauge; Lwt.return_unit; ]

end
